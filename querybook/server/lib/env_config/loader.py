import json
import os
import re
from typing import Any, Dict, List, Optional, Tuple

from lib.logger import get_logger
from lib.env_config.exc import EnvConfigError
from lib.env_config.models import EnvQueryEngine, EnvQueryMetastore

LOG = get_logger(__file__)

# Default prefixes. Configurable via env to handle naming conflicts.
QUERY_ENGINE_PREFIX = os.environ.get(
    "QUERYBOOK_QUERY_ENGINE_PREFIX", "QUERYBOOK_QUERY_ENGINE_"
)
METASTORE_PREFIX = os.environ.get(
    "QUERYBOOK_METASTORE_PREFIX", "QUERYBOOK_METASTORE_"
)

# Matches ${VAR} for substitution. Does not support ${VAR:-default} yet.
_VAR_RE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")


def _substitute_string(value: str, env: Dict[str, str]) -> str:
    """Replace ${VAR} occurrences in `value` using `env`.

    Raises EnvConfigError if a referenced variable is missing.
    """
    missing = []

    def replace(match: re.Match) -> str:
        var_name = match.group(1)
        if var_name not in env:
            missing.append(var_name)
            return match.group(0)
        return env[var_name]

    result = _VAR_RE.sub(replace, value)
    if missing:
        raise EnvConfigError(
            f"Undefined env var(s) in substitution: {', '.join(sorted(set(missing)))}"
        )
    return result


def _substitute_recursive(node: Any, env: Dict[str, str]) -> Any:
    """Walk dict/list/str nodes and apply substitution to every string."""
    if isinstance(node, str):
        return _substitute_string(node, env)
    if isinstance(node, dict):
        return {k: _substitute_recursive(v, env) for k, v in node.items()}
    if isinstance(node, list):
        return [_substitute_recursive(v, env) for v in node]
    return node


def _parse_json(raw: str, var_name: str) -> Dict[str, Any]:
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as e:
        # Do not include raw value in the error — it may contain secrets.
        raise EnvConfigError(
            f"Invalid JSON in env var {var_name}: {e.msg} at line {e.lineno}"
        )
    if not isinstance(parsed, dict):
        raise EnvConfigError(
            f"Env var {var_name} must contain a JSON object, got {type(parsed).__name__}"
        )
    return parsed


def _validate_engine(name: str, data: Dict[str, Any]) -> None:
    required = ["language", "executor", "executor_params"]
    missing = [f for f in required if f not in data]
    if missing:
        raise EnvConfigError(
            f"Engine '{name}' missing required fields: {', '.join(missing)}"
        )
    if not isinstance(data["executor_params"], dict):
        raise EnvConfigError(f"Engine '{name}': executor_params must be an object")
    if "feature_params" in data and not isinstance(data["feature_params"], dict):
        raise EnvConfigError(f"Engine '{name}': feature_params must be an object")
    if "environments" in data and not isinstance(data["environments"], list):
        raise EnvConfigError(f"Engine '{name}': environments must be a list of names")


def _validate_metastore(name: str, data: Dict[str, Any]) -> None:
    required = ["loader", "metastore_params"]
    missing = [f for f in required if f not in data]
    if missing:
        raise EnvConfigError(
            f"Metastore '{name}' missing required fields: {', '.join(missing)}"
        )
    if not isinstance(data["metastore_params"], dict):
        raise EnvConfigError(f"Metastore '{name}': metastore_params must be an object")
    if "acl_control" in data and not isinstance(data["acl_control"], dict):
        raise EnvConfigError(f"Metastore '{name}': acl_control must be an object")
    if "update_cron" in data and not isinstance(data["update_cron"], str):
        raise EnvConfigError(f"Metastore '{name}': update_cron must be a string")


def _slug_to_name(slug: str, override: Optional[str]) -> str:
    """Pick the canonical name. Explicit `name` field in JSON wins,
    otherwise lowercase the env-var slug."""
    if override:
        return override
    return slug.lower()


def _load_prefixed(
    env: Dict[str, str], prefix: str
) -> List[Tuple[str, str, Dict[str, Any]]]:
    """Return list of (slug, var_name, parsed_dict) for every env var with given prefix.

    Failures on individual entries are logged and skipped — they don't abort the whole load.
    """
    out: List[Tuple[str, str, Dict[str, Any]]] = []
    for var_name, raw in env.items():
        if not var_name.startswith(prefix):
            continue
        slug = var_name[len(prefix):]
        if not slug:
            continue
        try:
            parsed = _parse_json(raw, var_name)
            substituted = _substitute_recursive(parsed, env)
            out.append((slug, var_name, substituted))
        except EnvConfigError as e:
            LOG.error(f"Skipping env config from {var_name}: {e}")
        except Exception as e:
            LOG.error(
                f"Unexpected error parsing {var_name}: {type(e).__name__}: {e}"
            )
    return out


def load_env_metastores(
    env: Optional[Dict[str, str]] = None,
) -> Dict[str, EnvQueryMetastore]:
    """Scan env for QUERYBOOK_METASTORE_* vars and return name → EnvQueryMetastore."""
    env = env if env is not None else dict(os.environ)
    result: Dict[str, EnvQueryMetastore] = {}
    for slug, var_name, data in _load_prefixed(env, METASTORE_PREFIX):
        try:
            _validate_metastore(slug, data)
            name = _slug_to_name(slug, data.get("name"))
            if name in result:
                LOG.warning(
                    f"Duplicate env metastore name '{name}' from {var_name}, overriding previous"
                )
            result[name] = EnvQueryMetastore(
                name=name,
                loader=data["loader"],
                metastore_params=data["metastore_params"],
                acl_control=data.get("acl_control"),
                update_cron=data.get("update_cron"),
            )
        except EnvConfigError as e:
            LOG.error(f"Skipping metastore from {var_name}: {e}")
    return result


def load_env_query_engines(
    env: Optional[Dict[str, str]] = None,
) -> Dict[str, EnvQueryEngine]:
    """Scan env for QUERYBOOK_QUERY_ENGINE_* vars and return name → EnvQueryEngine."""
    env = env if env is not None else dict(os.environ)
    result: Dict[str, EnvQueryEngine] = {}
    for slug, var_name, data in _load_prefixed(env, QUERY_ENGINE_PREFIX):
        try:
            _validate_engine(slug, data)
            name = _slug_to_name(slug, data.get("name"))
            if name in result:
                LOG.warning(
                    f"Duplicate env query engine name '{name}' from {var_name}, overriding previous"
                )
            result[name] = EnvQueryEngine(
                name=name,
                description=data.get("description"),
                language=data["language"],
                executor=data["executor"],
                executor_params=data["executor_params"],
                feature_params=data.get("feature_params"),
                metastore_name=data.get("metastore_name"),
                environments=data.get("environments"),
            )
        except EnvConfigError as e:
            LOG.error(f"Skipping query engine from {var_name}: {e}")
    return result
