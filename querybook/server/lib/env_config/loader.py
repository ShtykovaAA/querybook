import json
import os
import re
from typing import Any, Dict, List, Optional, Tuple

import yaml

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

# Path to a YAML file with `query_engines:` and `metastores:` lists.
# Set via QUERYBOOK_CONNECTIONS_FILE env var. Optional — skipped if unset
# or pointing at a missing file.
CONNECTIONS_FILE_ENV = "QUERYBOOK_CONNECTIONS_FILE"

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


# --- YAML file source ----------------------------------------------------
#
# A YAML file is more readable than long QUERYBOOK_QUERY_ENGINE_<NAME>=<json>
# env vars, especially for declaring many connections at once. The file's
# location is configured via QUERYBOOK_CONNECTIONS_FILE.
#
# Secrets stay in env vars: ${VAR} placeholders inside the YAML are resolved
# from os.environ at startup, exactly like in the JSON env-var path. So a
# typical k8s setup is:
#   - connections.yml mounted from a ConfigMap (no secrets)
#   - QUERYBOOK_*_PASSWORD vars sourced from a Secret
# and the loader merges them.
#
# File schema:
#   query_engines:
#     - name: prod_pg
#       language: postgresql
#       executor: sqlalchemy
#       executor_params:
#         connection_string: "postgresql://u:${PG_PASS}@h/d"
#       environments: [analytics]
#   metastores:
#     - name: prod_pg_meta
#       loader: SqlAlchemyMetastoreLoader
#       metastore_params:
#         connection_string: "postgresql://u:${PG_PASS}@h/d"
#       update_cron: "0 * * * *"


def _read_connections_file(
    path: str, env: Dict[str, str]
) -> Optional[Dict[str, Any]]:
    """Read and parse a connections YAML file. Returns None if the file
    does not exist (treated as a soft skip — the path may simply be unset).
    Raises EnvConfigError on parse / format errors.

    ${VAR} substitution is applied AFTER YAML parsing, walking the parsed
    structure. This way ${VAR}-looking sequences inside YAML comments
    (which often appear in documentation blocks) are stripped by the
    parser and never trigger the substitution.
    """
    if not os.path.exists(path):
        LOG.info(f"Connections file '{path}' not found, skipping file source")
        return None
    try:
        with open(path, "r") as fp:
            raw = fp.read()
    except OSError as e:
        raise EnvConfigError(f"Failed to read connections file '{path}': {e}")
    try:
        parsed = yaml.safe_load(raw)
    except yaml.YAMLError as e:
        raise EnvConfigError(f"YAML parse error in '{path}': {e}")
    if parsed is None:
        return {}
    if not isinstance(parsed, dict):
        raise EnvConfigError(
            f"Connections file '{path}' must contain a top-level mapping, "
            f"got {type(parsed).__name__}"
        )
    try:
        substituted = _substitute_recursive(parsed, env)
    except EnvConfigError as e:
        raise EnvConfigError(f"In connections file '{path}': {e}")
    return substituted


def load_file_metastores(
    path: Optional[str] = None,
    env: Optional[Dict[str, str]] = None,
) -> Dict[str, EnvQueryMetastore]:
    """Read metastores from the connections YAML file.

    Returns empty dict if no path is set or file is absent.
    """
    env = env if env is not None else dict(os.environ)
    path = path if path is not None else env.get(CONNECTIONS_FILE_ENV)
    if not path:
        return {}

    try:
        data = _read_connections_file(path, env)
    except EnvConfigError as e:
        LOG.error(f"Failed to load connections file: {e}")
        return {}
    if data is None:
        return {}

    result: Dict[str, EnvQueryMetastore] = {}
    for entry in data.get("metastores", []) or []:
        if not isinstance(entry, dict) or "name" not in entry:
            LOG.error(
                f"Skipping metastore entry without 'name' in {path}"
            )
            continue
        try:
            _validate_metastore(entry["name"], entry)
            name = entry["name"]
            if name in result:
                LOG.warning(
                    f"Duplicate metastore name '{name}' in {path}, overriding previous"
                )
            result[name] = EnvQueryMetastore(
                name=name,
                loader=entry["loader"],
                metastore_params=entry["metastore_params"],
                acl_control=entry.get("acl_control"),
                update_cron=entry.get("update_cron"),
            )
        except EnvConfigError as e:
            LOG.error(f"Skipping metastore '{entry.get('name')}' from {path}: {e}")
    return result


def load_file_query_engines(
    path: Optional[str] = None,
    env: Optional[Dict[str, str]] = None,
) -> Dict[str, EnvQueryEngine]:
    """Read query engines from the connections YAML file."""
    env = env if env is not None else dict(os.environ)
    path = path if path is not None else env.get(CONNECTIONS_FILE_ENV)
    if not path:
        return {}

    try:
        data = _read_connections_file(path, env)
    except EnvConfigError as e:
        LOG.error(f"Failed to load connections file: {e}")
        return {}
    if data is None:
        return {}

    result: Dict[str, EnvQueryEngine] = {}
    for entry in data.get("query_engines", []) or []:
        if not isinstance(entry, dict) or "name" not in entry:
            LOG.error(
                f"Skipping query engine entry without 'name' in {path}"
            )
            continue
        try:
            _validate_engine(entry["name"], entry)
            name = entry["name"]
            if name in result:
                LOG.warning(
                    f"Duplicate query engine name '{name}' in {path}, overriding previous"
                )
            result[name] = EnvQueryEngine(
                name=name,
                description=entry.get("description"),
                language=entry["language"],
                executor=entry["executor"],
                executor_params=entry["executor_params"],
                feature_params=entry.get("feature_params"),
                metastore_name=entry.get("metastore_name"),
                environments=entry.get("environments"),
            )
        except EnvConfigError as e:
            LOG.error(f"Skipping query engine '{entry.get('name')}' from {path}: {e}")
    return result


def load_all_metastores(
    env: Optional[Dict[str, str]] = None,
) -> Dict[str, EnvQueryMetastore]:
    """Merge metastores from the YAML file (if any) and env vars.

    Env vars win on name collision — they are the more explicit override
    channel and easier to set per-pod / per-environment.
    """
    file_ms = load_file_metastores(env=env)
    env_ms = load_env_metastores(env=env)
    for name in env_ms.keys() & file_ms.keys():
        LOG.warning(
            f"Metastore '{name}' defined in both connections file and env var; env var wins"
        )
    return {**file_ms, **env_ms}


def load_all_query_engines(
    env: Optional[Dict[str, str]] = None,
) -> Dict[str, EnvQueryEngine]:
    """Merge query engines from the YAML file (if any) and env vars.
    Env vars override on name collision."""
    file_eng = load_file_query_engines(env=env)
    env_eng = load_env_query_engines(env=env)
    for name in env_eng.keys() & file_eng.keys():
        LOG.warning(
            f"Query engine '{name}' defined in both connections file and env var; env var wins"
        )
    return {**file_eng, **env_eng}
