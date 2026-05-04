"""Sync env-managed query engines and metastores to DB as shadow rows.

Why this exists:
    Env-managed objects live only in process memory with synthetic IDs from
    the reserved range (>= ENV_ID_BASE). Several child tables (data_schema,
    query_execution, etc.) have FK constraints referencing query_engine.id /
    query_metastore.id. Without a parent row in those tables, any insert
    against the child fails with FK violation.

    sync_env_to_db() inserts a "shadow" parent row containing only
    non-secret fields (id, name, language, executor, loader). Secret params
    (executor_params, metastore_params, acl_control) are NEVER written to
    these rows — they live in env vars and are read through the registry at
    runtime.

Behavior on restart:
    - same env config            → no-op
    - changed connection_string  → no-op (params not stored in shadow)
    - changed language/executor  → UPDATE shadow row
    - removed env var            → soft-delete (deleted_at = now)
    - returned env var           → revive (deleted_at = NULL)
    - new env var                → INSERT new shadow row
    - hash collision             → fail-fast on startup
    - name conflict with DB row  → fail-fast on startup
"""
from datetime import datetime
from typing import Dict, List

from app.db import DBSession
from lib.env_config.exc import EnvConfigError
from lib.env_config.models import (
    ENV_ID_BASE,
    EnvQueryEngine,
    EnvQueryMetastore,
)
from lib.env_config.registry import (
    get_env_metastores,
    get_env_query_engines,
)
from lib.logger import get_logger
from models.admin import QueryEngine, QueryEngineEnvironment, QueryMetastore

# Imports of logic.* are deferred inside functions to avoid a circular
# import: app/flask_app.py imports this module at module load, and
# logic.schedule imports back from app.flask_app.

LOG = get_logger(__file__)


# Fields actually stored in shadow rows. Anything not listed here is not
# written. Crucially, executor_params / metastore_params / acl_control are
# excluded — they stay in env vars only.
_ENGINE_SHADOW_FIELDS = [
    "id",
    "name",
    "description",
    "language",
    "executor",
    "executor_params",
    "feature_params",
    "metastore_id",
    "deleted_at",
    "updated_at",
]
_METASTORE_SHADOW_FIELDS = [
    "id",
    "name",
    "loader",
    "metastore_params",
    "acl_control",
    "deleted_at",
    "updated_at",
]

# Fields updated on conflict. Same as insert fields minus `id`.
_ENGINE_UPDATE_FIELDS = [f for f in _ENGINE_SHADOW_FIELDS if f != "id"]
_METASTORE_UPDATE_FIELDS = [f for f in _METASTORE_SHADOW_FIELDS if f != "id"]


def _engine_shadow_values(env_engine: EnvQueryEngine, now: datetime) -> Dict:
    return {
        "id": env_engine.id,
        "name": env_engine.name,
        "description": env_engine.description or "[env-managed]",
        "language": env_engine.language,
        "executor": env_engine.executor,
        "executor_params": {},  # never store secrets
        "feature_params": {},
        "metastore_id": env_engine.metastore_id,
        "deleted_at": None,
        "updated_at": now,
    }


def _metastore_shadow_values(env_ms: EnvQueryMetastore, now: datetime) -> Dict:
    return {
        "id": env_ms.id,
        "name": env_ms.name,
        "loader": env_ms.loader,
        "metastore_params": {},  # never store secrets
        "acl_control": {},
        "deleted_at": None,
        "updated_at": now,
    }


def _upsert(session, table, values: Dict, update_fields: List[str]) -> None:
    """Dialect-aware upsert by primary key (id).

    PostgreSQL: INSERT ... ON CONFLICT (id) DO UPDATE SET <col> = EXCLUDED.<col>
    MySQL:      INSERT ... ON DUPLICATE KEY UPDATE <col> = VALUES(<col>)
    SQLite:     SELECT, then INSERT or UPDATE in transaction.
    """
    dialect = session.bind.dialect.name

    if dialect == "postgresql":
        from sqlalchemy.dialects.postgresql import insert as pg_insert

        stmt = pg_insert(table).values(**values)
        stmt = stmt.on_conflict_do_update(
            index_elements=[table.__table__.c.id],
            set_={k: stmt.excluded[k] for k in update_fields},
        )
        session.execute(stmt)
    elif dialect == "mysql":
        from sqlalchemy.dialects.mysql import insert as mysql_insert

        stmt = mysql_insert(table).values(**values)
        stmt = stmt.on_duplicate_key_update(
            **{k: stmt.inserted[k] for k in update_fields}
        )
        session.execute(stmt)
    else:
        existing = session.query(table).filter(table.id == values["id"]).first()
        if existing is None:
            session.add(table(**values))
        else:
            for k in update_fields:
                if k in values:
                    setattr(existing, k, values[k])


def _check_id_collisions(
    engines: List[EnvQueryEngine], metastores: List[EnvQueryMetastore]
) -> None:
    """Fail-fast if compute_env_id produced the same ID for different names."""
    seen: Dict[int, str] = {}
    for e in engines:
        key = ("engine", e.id)
        if e.id in seen and seen[e.id] != f"engine:{e.name}":
            raise EnvConfigError(
                f"Hash collision in env config: engine '{e.name}' and "
                f"'{seen[e.id]}' both produce id {e.id}. Rename one of them."
            )
        seen[e.id] = f"engine:{e.name}"

    seen_ms: Dict[int, str] = {}
    for m in metastores:
        if m.id in seen_ms and seen_ms[m.id] != f"metastore:{m.name}":
            raise EnvConfigError(
                f"Hash collision in env config: metastore '{m.name}' and "
                f"'{seen_ms[m.id]}' both produce id {m.id}. Rename one of them."
            )
        seen_ms[m.id] = f"metastore:{m.name}"


def _check_name_conflicts(
    session,
    engines: List[EnvQueryEngine],
    metastores: List[EnvQueryMetastore],
) -> None:
    """Fail-fast if env name matches an existing DB row that is NOT a shadow.

    A DB row is a shadow if its id is in the env range and its name matches
    an env object — that's the same object, fine. A non-shadow DB row with
    the same name as an env object is a real conflict (UNIQUE on `name`).
    """
    env_engine_names = {e.name: e.id for e in engines}
    if env_engine_names:
        rows = (
            session.query(QueryEngine.id, QueryEngine.name)
            .filter(QueryEngine.name.in_(env_engine_names.keys()))
            .all()
        )
        for row_id, row_name in rows:
            expected_id = env_engine_names[row_name]
            if row_id != expected_id:
                raise EnvConfigError(
                    f"Query engine name '{row_name}' is used by a DB-managed "
                    f"record (id={row_id}). Rename either the env var or the "
                    f"DB record before starting."
                )

    env_ms_names = {m.name: m.id for m in metastores}
    if env_ms_names:
        rows = (
            session.query(QueryMetastore.id, QueryMetastore.name)
            .filter(QueryMetastore.name.in_(env_ms_names.keys()))
            .all()
        )
        for row_id, row_name in rows:
            expected_id = env_ms_names[row_name]
            if row_id != expected_id:
                raise EnvConfigError(
                    f"Metastore name '{row_name}' is used by a DB-managed "
                    f"record (id={row_id}). Rename either the env var or the "
                    f"DB record before starting."
                )


def _mark_orphans_deleted(session, live_ids: List[int], model) -> int:
    """Soft-delete shadow rows whose env source is gone.

    A row is considered an orphan if its id is in the env range but is not
    in `live_ids` (the IDs of currently loaded env objects). Children
    (query_execution, data_schema, etc.) are preserved — only the parent's
    deleted_at is set, no CASCADE triggered.
    """
    now = datetime.utcnow()
    result = (
        session.query(model)
        .filter(model.id >= ENV_ID_BASE)
        .filter(~model.id.in_(live_ids) if live_ids else True)
        .filter(model.deleted_at.is_(None))
        .update(
            {model.deleted_at: now, model.updated_at: now},
            synchronize_session=False,
        )
    )
    # For engines, also drop env-environment bindings — the engine should
    # disappear from the dropdown immediately, even before cleanup endpoint
    # is called.
    if model is QueryEngine:
        orphan_ids = [
            row_id
            for (row_id,) in session.query(QueryEngine.id)
            .filter(QueryEngine.id >= ENV_ID_BASE)
            .filter(~QueryEngine.id.in_(live_ids) if live_ids else True)
            .all()
        ]
        if orphan_ids:
            session.query(QueryEngineEnvironment).filter(
                QueryEngineEnvironment.query_engine_id.in_(orphan_ids)
            ).delete(synchronize_session=False)
    return result


def _sync_metastore_schedules(session, metastores: List[EnvQueryMetastore]) -> None:
    """Create / update / delete task_schedule rows from metastore.update_cron field."""
    from logic.admin import get_metastore_schedule_job_name
    from logic.schedule import (
        create_task_schedule,
        delete_task_schedule,
        get_task_schedule_by_name,
    )

    for ms in metastores:
        update_cron = getattr(ms, "update_cron", None)
        schedule_name = get_metastore_schedule_job_name(ms.id)
        existing = get_task_schedule_by_name(schedule_name, session=session)

        if update_cron:
            if existing is None:
                create_task_schedule(
                    name=schedule_name,
                    task="tasks.update_metastore.update_metastore",
                    cron=update_cron,
                    args=[ms.id],
                    commit=False,
                    session=session,
                )
                LOG.info(f"Created update schedule for env metastore '{ms.name}'")
            elif existing.cron != update_cron:
                existing.cron = update_cron
                LOG.info(f"Updated cron for env metastore '{ms.name}': {update_cron}")
        elif existing is not None:
            delete_task_schedule(existing.id, commit=False, session=session)
            LOG.info(f"Removed update schedule for env metastore '{ms.name}'")


def sync_env_to_db(session=None) -> Dict[str, int]:
    """Main entry point. Idempotent.

    Returns a counters dict for logging: {created, updated, orphaned, revived, total}.
    Caller may swallow the returned exception to allow partial degradation, but
    the recommended behavior is fail-fast — crash the process so the deploy
    pipeline catches the misconfig.
    """
    engines = get_env_query_engines()
    metastores = get_env_metastores()

    _check_id_collisions(engines, metastores)

    counters = {"engines": 0, "metastores": 0, "orphans": 0, "revived": 0}

    if session is not None:
        _do_sync(session, engines, metastores, counters)
    else:
        with DBSession() as s:
            _do_sync(s, engines, metastores, counters)
            s.commit()

    LOG.info(
        f"env_config sync: engines={counters['engines']} metastores={counters['metastores']} "
        f"orphans_marked={counters['orphans']} revived={counters['revived']}"
    )
    return counters


def _do_sync(
    session,
    engines: List[EnvQueryEngine],
    metastores: List[EnvQueryMetastore],
    counters: Dict[str, int],
) -> None:
    _check_name_conflicts(session, engines, metastores)
    now = datetime.utcnow()

    # Metastores first — engines may reference their IDs.
    for ms in metastores:
        _upsert(
            session,
            QueryMetastore,
            _metastore_shadow_values(ms, now),
            _METASTORE_UPDATE_FIELDS,
        )
        counters["metastores"] += 1

    for engine in engines:
        _upsert(
            session,
            QueryEngine,
            _engine_shadow_values(engine, now),
            _ENGINE_UPDATE_FIELDS,
        )
        counters["engines"] += 1

    # Revive previously-orphaned rows that returned. This must run BEFORE
    # marking orphans, because the upserts above already cleared deleted_at
    # for live env objects — so this is a no-op for them. The targets here
    # are rows whose env objects came back in this same sync.
    # (Already handled by the upsert which sets deleted_at=None.)

    # Orphan: shadow rows in env range with IDs not in live set.
    live_engine_ids = [e.id for e in engines]
    live_metastore_ids = [m.id for m in metastores]
    counters["orphans"] += _mark_orphans_deleted(
        session, live_engine_ids, QueryEngine
    )
    counters["orphans"] += _mark_orphans_deleted(
        session, live_metastore_ids, QueryMetastore
    )

    # Schedules for env metastores with update_cron set.
    _sync_metastore_schedules(session, metastores)

    # Bind env engines to environments via query_engine_environment.
    # Permission checks (verify_query_engine_permission) join on this
    # table, so without rows here env-engines are 404 to all users.
    _sync_engine_environment_bindings(session, engines)


def _sync_engine_environment_bindings(session, engines: List[EnvQueryEngine]) -> None:
    """Replace query_engine_environment rows for each env engine to match
    the `environments` list in the env JSON.

    For env engines we own the bindings — DELETE existing and INSERT fresh.
    For DB engines this table is unaffected.
    """
    from logic.environment import get_environment_by_name
    from sqlalchemy import func

    for engine in engines:
        # Resolve names to environment rows.
        target_env_ids: List[int] = []
        for env_name in engine.environment_names:
            env_row = get_environment_by_name(env_name, session=session)
            if env_row is None:
                LOG.warning(
                    f"Env engine '{engine.name}' references unknown environment "
                    f"'{env_name}'; skipping binding."
                )
                continue
            target_env_ids.append(env_row.id)

        existing = (
            session.query(QueryEngineEnvironment)
            .filter(QueryEngineEnvironment.query_engine_id == engine.id)
            .all()
        )
        existing_env_ids = {b.environment_id for b in existing}

        # Drop bindings no longer in env JSON.
        for binding in existing:
            if binding.environment_id not in target_env_ids:
                session.delete(binding)

        # Add missing bindings, appending after existing engines for that env.
        for env_id in target_env_ids:
            if env_id in existing_env_ids:
                continue
            max_order = (
                session.query(func.max(QueryEngineEnvironment.engine_order))
                .filter(QueryEngineEnvironment.environment_id == env_id)
                .scalar()
                or 0
            )
            session.add(
                QueryEngineEnvironment(
                    query_engine_id=engine.id,
                    environment_id=env_id,
                    engine_order=max_order + 1,
                )
            )


def list_orphans(session=None) -> Dict[str, List[Dict]]:
    """Return shadow rows in the env range that have no live env object.

    Used by the cleanup endpoint to show admins what's stale.
    """
    live_engine_ids = {e.id for e in get_env_query_engines()}
    live_metastore_ids = {m.id for m in get_env_metastores()}

    if session is None:
        with DBSession() as s:
            return _list_orphans_inner(s, live_engine_ids, live_metastore_ids)
    return _list_orphans_inner(session, live_engine_ids, live_metastore_ids)


def _list_orphans_inner(session, live_engine_ids, live_metastore_ids) -> Dict:
    engines = (
        session.query(QueryEngine)
        .filter(QueryEngine.id >= ENV_ID_BASE)
        .filter(~QueryEngine.id.in_(live_engine_ids) if live_engine_ids else True)
        .all()
    )
    metastores = (
        session.query(QueryMetastore)
        .filter(QueryMetastore.id >= ENV_ID_BASE)
        .filter(
            ~QueryMetastore.id.in_(live_metastore_ids) if live_metastore_ids else True
        )
        .all()
    )
    return {
        "engines": [
            {
                "id": e.id,
                "name": e.name,
                "language": e.language,
                "executor": e.executor,
                "deleted_at": e.deleted_at,
            }
            for e in engines
        ],
        "metastores": [
            {
                "id": m.id,
                "name": m.name,
                "loader": m.loader,
                "deleted_at": m.deleted_at,
            }
            for m in metastores
        ],
    }


def delete_orphan_engine(engine_id: int, session=None) -> bool:
    """Hard-delete an orphan engine. CASCADE removes children (query_execution etc).

    Returns True if deleted, False if not found or not an orphan.
    """
    live_engine_ids = {e.id for e in get_env_query_engines()}
    if engine_id < ENV_ID_BASE or engine_id in live_engine_ids:
        return False

    if session is None:
        with DBSession() as s:
            return _delete_orphan_inner(s, engine_id, QueryEngine)
    return _delete_orphan_inner(session, engine_id, QueryEngine)


def delete_orphan_metastore(metastore_id: int, session=None) -> bool:
    live_metastore_ids = {m.id for m in get_env_metastores()}
    if metastore_id < ENV_ID_BASE or metastore_id in live_metastore_ids:
        return False

    if session is None:
        with DBSession() as s:
            return _delete_orphan_inner(s, metastore_id, QueryMetastore)
    return _delete_orphan_inner(session, metastore_id, QueryMetastore)


def _delete_orphan_inner(session, obj_id: int, model) -> bool:
    row = session.query(model).filter(model.id == obj_id).first()
    if row is None:
        return False
    session.delete(row)
    session.commit()
    return True
