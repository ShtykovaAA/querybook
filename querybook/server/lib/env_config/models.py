from datetime import datetime
from typing import Any, Dict, List, Optional


# ID range reserved for env-managed objects.
# Far above any realistic auto-increment value, but still within signed INT.
ENV_ID_BASE = 1_900_000_000
ENV_ID_MASK = 0x07FFFFFF  # 27 bits, ~134M slots


def compute_env_id(name: str) -> int:
    """Deterministic positive ID for an env-managed object based on its name."""
    h = 0
    for ch in name:
        h = (h * 31 + ord(ch)) & 0xFFFFFFFF
    return ENV_ID_BASE + (h & ENV_ID_MASK)


class EnvQueryMetastore:
    """Lightweight stand-in for the QueryMetastore ORM model.

    Quacks like the SQLAlchemy model: same attributes and serialization
    methods, so callers in logic/datasources don't need to special-case it.
    """

    def __init__(
        self,
        name: str,
        loader: str,
        metastore_params: Dict[str, Any],
        acl_control: Optional[Dict[str, Any]] = None,
        update_cron: Optional[str] = None,
    ):
        self.id = compute_env_id(f"metastore:{name}")
        self.name = name
        self.loader = loader
        self.metastore_params = metastore_params
        self.acl_control = acl_control or {}
        self.update_cron = update_cron
        self.created_at = datetime.utcnow()
        self.updated_at = self.created_at
        self.deleted_at = None

    def to_dict(self, with_flags: bool = False) -> Dict[str, Any]:
        from lib.metastore import get_metastore_loader_class_by_name

        loader_class = get_metastore_loader_class_by_name(self.loader)
        d = {
            "id": self.id,
            "name": self.name,
            "config": loader_class.loader_config.to_dict(),
            "owner_types": [t._asdict() for t in loader_class.get_table_owner_types()],
            "is_env_managed": True,
        }
        if with_flags:
            d["flags"] = {"has_data_element": False}
        return d

    def to_dict_admin(self) -> Dict[str, Any]:
        # IMPORTANT: returns real metastore_params — the loader reads this
        # dict at runtime to open the metastore connection.
        # Secret masking for the admin API happens at the datasource layer
        # (see datasources/admin.py:_mask_env_managed_metastore).
        return {
            "id": self.id,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "deleted_at": self.deleted_at,
            "name": self.name,
            "loader": self.loader,
            "metastore_params": self.metastore_params,
            "acl_control": self.acl_control,
            "is_env_managed": True,
        }


class EnvQueryEngine:
    """Lightweight stand-in for the QueryEngine ORM model."""

    def __init__(
        self,
        name: str,
        language: str,
        executor: str,
        executor_params: Dict[str, Any],
        description: Optional[str] = None,
        feature_params: Optional[Dict[str, Any]] = None,
        metastore_name: Optional[str] = None,
        environments: Optional[List[str]] = None,
    ):
        self.id = compute_env_id(f"engine:{name}")
        self.name = name
        self.description = description
        self.language = language
        self.executor = executor
        self.executor_params = executor_params
        self.feature_params = feature_params or {}
        self._metastore_name = metastore_name
        self._environment_names = environments or []
        self.created_at = datetime.utcnow()
        self.updated_at = self.created_at
        self.deleted_at = None

    @property
    def metastore_name(self) -> Optional[str]:
        return self._metastore_name

    @property
    def environment_names(self) -> List[str]:
        return list(self._environment_names)

    @property
    def metastore_id(self) -> Optional[int]:
        # Resolve metastore_name → id via registry first, then DB.
        if self._metastore_name is None:
            return None
        from lib.env_config.registry import get_env_metastore_by_name

        env_ms = get_env_metastore_by_name(self._metastore_name)
        if env_ms is not None:
            return env_ms.id

        # Fall back to DB by name.
        from logic.admin import get_query_metastore_by_name

        try:
            db_ms = get_query_metastore_by_name(self._metastore_name)
        except Exception:
            return None
        return db_ms.id if db_ms is not None else None

    @property
    def metastore(self):
        if self._metastore_name is None:
            return None
        from lib.env_config.registry import get_env_metastore_by_name

        env_ms = get_env_metastore_by_name(self._metastore_name)
        if env_ms is not None:
            return env_ms
        from logic.admin import get_query_metastore_by_name

        try:
            return get_query_metastore_by_name(self._metastore_name)
        except Exception:
            return None

    @property
    def environments(self):
        if not self._environment_names:
            return []
        # Resolve environment names to Environment ORM objects on demand.
        from logic.environment import get_environment_by_name

        result = []
        for env_name in self._environment_names:
            try:
                env = get_environment_by_name(env_name)
            except Exception:
                env = None
            if env is not None:
                result.append(env)
        return result

    def get_engine_params(self) -> Dict[str, Any]:
        return self.executor_params

    def get_feature_params(self) -> Dict[str, Any]:
        return self.feature_params or {}

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "language": self.language,
            "description": self.description,
            "metastore_id": self.metastore_id,
            "feature_params": self.get_feature_params(),
            "executor": self.executor,
            "is_env_managed": True,
        }

    def to_dict_admin(self) -> Dict[str, Any]:
        # IMPORTANT: returns real executor_params. Secret masking for the
        # admin API happens at the datasource layer
        # (see datasources/admin.py:_mask_env_managed_engine).
        return {
            "id": self.id,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "deleted_at": self.deleted_at,
            "name": self.name,
            "language": self.language,
            "description": self.description,
            "metastore_id": self.metastore_id,
            "executor": self.executor,
            "executor_params": self.executor_params,
            "feature_params": self.get_feature_params(),
            "environments": self.environments,
            "is_env_managed": True,
        }
