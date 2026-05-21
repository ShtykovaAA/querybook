from app.datasource import register, admin_only, api_assert
from app.db import DBSession
from const.admin import AdminItemType, AdminOperation
from datasources.admin_audit_log import with_admin_audit_log
from lib.env_config import (
    is_in_env_id_range,
    is_orphan_engine_id,
    is_orphan_metastore_id,
)
from lib.env_config.db_sync import (
    delete_orphan_engine,
    delete_orphan_metastore,
    list_orphans,
)


@register("/admin/env_config/", methods=["GET"])
@admin_only
def get_env_config_overview():
    """Return live env-managed objects + orphan shadow rows."""
    from lib.env_config import get_env_metastores, get_env_query_engines

    live_engines = [
        {"id": e.id, "name": e.name, "language": e.language, "executor": e.executor}
        for e in get_env_query_engines()
    ]
    live_metastores = [
        {"id": m.id, "name": m.name, "loader": m.loader} for m in get_env_metastores()
    ]
    with DBSession() as session:
        orphans = list_orphans(session=session)

    return {
        "live": {"engines": live_engines, "metastores": live_metastores},
        "orphans": orphans,
    }


@register("/admin/env_config/orphans/engine/<int:id>/", methods=["DELETE"])
@admin_only
@with_admin_audit_log(AdminItemType.QueryEngine, AdminOperation.DELETE)
def delete_orphan_engine_endpoint(id):
    api_assert(
        is_in_env_id_range(id),
        message=f"Engine id {id} is not in the env-managed range.",
        status_code=400,
    )
    api_assert(
        is_orphan_engine_id(id),
        message=(
            f"Engine id {id} is currently backed by a live env config "
            f"and cannot be cleaned up. Remove the env var first."
        ),
        status_code=409,
    )
    deleted = delete_orphan_engine(id)
    api_assert(deleted, message="Orphan engine not found", status_code=404)


@register("/admin/env_config/orphans/metastore/<int:id>/", methods=["DELETE"])
@admin_only
@with_admin_audit_log(AdminItemType.QueryMetastore, AdminOperation.DELETE)
def delete_orphan_metastore_endpoint(id):
    api_assert(
        is_in_env_id_range(id),
        message=f"Metastore id {id} is not in the env-managed range.",
        status_code=400,
    )
    api_assert(
        is_orphan_metastore_id(id),
        message=(
            f"Metastore id {id} is currently backed by a live env config "
            f"and cannot be cleaned up. Remove the env var first."
        ),
        status_code=409,
    )
    deleted = delete_orphan_metastore(id)
    api_assert(deleted, message="Orphan metastore not found", status_code=404)
