import threading
from typing import Dict, List, Optional

from lib.env_config.loader import load_all_metastores, load_all_query_engines
from lib.env_config.models import ENV_ID_BASE, EnvQueryEngine, EnvQueryMetastore


_lock = threading.Lock()
_engines_by_name: Optional[Dict[str, EnvQueryEngine]] = None
_engines_by_id: Optional[Dict[int, EnvQueryEngine]] = None
_metastores_by_name: Optional[Dict[str, EnvQueryMetastore]] = None
_metastores_by_id: Optional[Dict[int, EnvQueryMetastore]] = None


def _ensure_loaded() -> None:
    global _engines_by_name, _engines_by_id, _metastores_by_name, _metastores_by_id
    if _engines_by_name is not None:
        return
    with _lock:
        if _engines_by_name is not None:
            return
        metastores = load_all_metastores()
        engines = load_all_query_engines()
        _metastores_by_name = metastores
        _metastores_by_id = {m.id: m for m in metastores.values()}
        _engines_by_name = engines
        _engines_by_id = {e.id: e for e in engines.values()}


def reload_registry() -> None:
    """Force re-scan of env vars. Intended for tests."""
    global _engines_by_name, _engines_by_id, _metastores_by_name, _metastores_by_id
    with _lock:
        _engines_by_name = None
        _engines_by_id = None
        _metastores_by_name = None
        _metastores_by_id = None
    _ensure_loaded()


def get_env_query_engines() -> List[EnvQueryEngine]:
    _ensure_loaded()
    return list(_engines_by_name.values())


def get_env_query_engine_by_id(engine_id: int) -> Optional[EnvQueryEngine]:
    _ensure_loaded()
    return _engines_by_id.get(engine_id)


def get_env_query_engine_by_name(name: str) -> Optional[EnvQueryEngine]:
    _ensure_loaded()
    return _engines_by_name.get(name)


def get_env_query_engines_by_environment_name(
    environment_name: str,
) -> List[EnvQueryEngine]:
    _ensure_loaded()
    return [
        engine
        for engine in _engines_by_name.values()
        if environment_name in engine.environment_names
    ]


def get_env_metastores() -> List[EnvQueryMetastore]:
    _ensure_loaded()
    return list(_metastores_by_name.values())


def get_env_metastore_by_id(metastore_id: int) -> Optional[EnvQueryMetastore]:
    _ensure_loaded()
    return _metastores_by_id.get(metastore_id)


def get_env_metastore_by_name(name: str) -> Optional[EnvQueryMetastore]:
    _ensure_loaded()
    return _metastores_by_name.get(name)


def is_env_managed_engine_id(engine_id: int) -> bool:
    _ensure_loaded()
    return engine_id in _engines_by_id


def is_env_managed_metastore_id(metastore_id: int) -> bool:
    _ensure_loaded()
    return metastore_id in _metastores_by_id


def is_in_env_id_range(obj_id: int) -> bool:
    """True if the id is in the reserved env range, regardless of whether
    a live env object currently backs it. Use this to identify shadow rows
    (live + orphans). For 'is this currently managed by env',
    use is_env_managed_engine_id / is_env_managed_metastore_id instead.
    """
    return obj_id >= ENV_ID_BASE


def is_orphan_engine_id(engine_id: int) -> bool:
    return is_in_env_id_range(engine_id) and not is_env_managed_engine_id(engine_id)


def is_orphan_metastore_id(metastore_id: int) -> bool:
    return is_in_env_id_range(metastore_id) and not is_env_managed_metastore_id(
        metastore_id
    )
