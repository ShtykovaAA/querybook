"""Helpers for the `run_on_main_engine_ids` schedule option.

A schedule may opt specific query cells into running on their engine's
`main_connection_string` instead of the engine's regular sandbox DSN.
The list is stored in `TaskSchedule.kwargs.run_on_main_engine_ids`.
"""
from typing import Iterable, List, Optional, Set

from app.db import with_session
from logic import admin as admin_logic


class InvalidMainEngineIdsError(Exception):
    pass


def normalize_run_on_main_engine_ids(value: Optional[Iterable[int]]) -> List[int]:
    """Return a sorted, deduped list. None/empty become []."""
    if not value:
        return []
    return sorted({int(v) for v in value})


@with_session
def assert_engines_can_run_on_main(
    engine_ids: List[int],
    data_doc,
    session=None,
) -> None:
    """Cross-validate run_on_main_engine_ids against the DataDoc's cells and
    each referenced engine's main_connection_string.

    Raises InvalidMainEngineIdsError on the first violation.
    """
    if not engine_ids:
        return

    doc_engine_ids: Set[int] = {
        cell.meta.get("engine")
        for cell in data_doc.get_query_cells()
        if cell.meta and cell.meta.get("engine") is not None
    }

    for engine_id in engine_ids:
        if engine_id not in doc_engine_ids:
            raise InvalidMainEngineIdsError(
                f"Engine {engine_id} is not used by any cell in this DataDoc"
            )
        engine = admin_logic.get_query_engine_by_id(engine_id, session=session)
        if engine is None:
            raise InvalidMainEngineIdsError(
                f"Engine {engine_id} not found"
            )
        if not getattr(engine, "main_connection_string", None):
            raise InvalidMainEngineIdsError(
                f"Engine {engine_id} has no main connection configured"
            )


def get_engine_main_connection_string(engine) -> Optional[str]:
    """Read main_connection_string from either an ORM QueryEngine or an
    EnvQueryEngine. Returns None if not configured."""
    if engine is None:
        return None
    return getattr(engine, "main_connection_string", None) or None
