import datetime

from celery import chain

from app.db import with_session, DBSession
from app.flask_app import celery, socketio

from const.db import (
    description_length,
)
from const.query_execution import QueryExecutionStatus, QueryExecutionType
from const.schedule import TaskRunStatus

from lib.logger import get_logger
from lib.query_analysis.templating import render_templated_query
from lib.scheduled_datadoc.exc import DataDocRunTimeoutException
from lib.scheduled_datadoc.export import export_datadoc
from lib.scheduled_datadoc.legacy import convert_if_legacy_datadoc_schedule
from lib.scheduled_datadoc.notification import notifiy_on_datadoc_complete
from lib.stats_logger import (
    DATADOC_SCHEDULED_RETRIES,
    DATADOC_SCHEDULED_RUNS,
    stats_logger,
)

from logic import datadoc as datadoc_logic
from logic import query_execution as qe_logic
from logic.schedule import (
    create_task_run_record_for_celery_task,
    get_data_doc_schedule_name,
    get_task_schedule_by_name,
    update_task_run_record,
)
from tasks.run_query import run_query_task

LOG = get_logger(__file__)
GENERIC_QUERY_FAILURE_MSG = "Execution did not finish successfully, workflow failed"
RUN_QUERY_TIME_LIMIT_BUFFER_SECONDS = 60
RETRY_BASE_DELAY_SECONDS = 60
RETRY_MAX_DELAY_SECONDS = 30 * 60


@celery.task(bind=True)
def run_datadoc(self, *args, **kwargs):
    """
    This function wraps run_datadoc_with_config to convert
    legacy schedule config to current
    """
    run_datadoc_with_config(self, *args, **convert_if_legacy_datadoc_schedule(kwargs))


def run_datadoc_with_config(
    self,
    doc_id,
    notifications=[],
    user_id=None,
    execution_type=QueryExecutionType.SCHEDULED.value,
    # Exporting related settings
    exports=[],
    *args,
    **kwargs,
):
    tasks_to_run = []
    record_id = None
    retry_parent_id = None

    timeout_seconds = kwargs.get("timeout_seconds")
    max_retries = kwargs.get("max_retries", 0) or 0
    attempt = kwargs.get("_attempt", 1)
    parent_run_record_id = kwargs.get("_parent_run_record_id")
    run_on_main_engine_ids = set(kwargs.get("run_on_main_engine_ids") or [])

    deadline_epoch = None
    if timeout_seconds:
        deadline_epoch = (
            datetime.datetime.utcnow()
            + datetime.timedelta(seconds=timeout_seconds)
        ).timestamp()

    # For retry attempts, honor a disabled schedule by aborting before work.
    # First attempts come from Celery Beat and only fire when enabled=True,
    # so we gate only on retries.
    if execution_type == QueryExecutionType.SCHEDULED.value and attempt > 1:
        schedule = get_task_schedule_by_name(get_data_doc_schedule_name(doc_id))
        if schedule is None or not schedule.enabled:
            return

    with DBSession() as session:
        data_doc = datadoc_logic.get_data_doc_by_id(doc_id, session=session)
        if not data_doc or data_doc.archived:
            return

        runner_id = user_id if user_id is not None else data_doc.owner_uid
        query_cells = data_doc.get_query_cells()

        # Create db entry record only for scheduled run
        if execution_type == QueryExecutionType.SCHEDULED.value:
            record_id = create_task_run_record_for_celery_task(
                self,
                attempt=attempt,
                parent_run_record_id=parent_run_record_id,
                session=session,
            )
            # For the first attempt we treat this record as the parent for
            # any future retries in the same chain.
            retry_parent_id = parent_run_record_id or record_id

        original_run_kwargs = {
            "doc_id": doc_id,
            "user_id": user_id,
            "execution_type": execution_type,
            "notifications": notifications,
            "exports": exports,
            "max_retries": max_retries,
        }
        if timeout_seconds:
            original_run_kwargs["timeout_seconds"] = timeout_seconds
        if run_on_main_engine_ids:
            original_run_kwargs["run_on_main_engine_ids"] = sorted(
                run_on_main_engine_ids
            )

        completion_params = {
            "doc_id": doc_id,
            "user_id": user_id,
            "record_id": record_id,
            "notifications": notifications,
            "exports": exports,
            "deadline_epoch": deadline_epoch,
            "max_retries": max_retries,
            "attempt": attempt,
            "parent_run_record_id": retry_parent_id,
            "original_run_kwargs": original_run_kwargs,
        }

        # Prepping chain jobs each unit is a [make_qe_task, run_query_task] combo
        for index, query_cell in enumerate(query_cells):
            engine_id = query_cell.meta["engine"]
            raw_query = query_cell.context

            # Skip empty cells
            if not raw_query or raw_query.isspace():
                continue

            try:
                query = render_templated_query(
                    raw_query,
                    data_doc.meta_variables,
                    engine_id,
                    session=session,
                )
            except Exception as e:
                on_datadoc_completion(
                    is_success=False,
                    error_msg=f"Error rendering template: {str(e)}",
                    **completion_params,
                )
                raise Exception(e)

            start_query_execution_kwargs = {
                "cell_id": query_cell.id,
                "query_execution_params": {
                    "query": query,
                    "engine_id": engine_id,
                    "uid": runner_id,
                    "use_main_connection": engine_id in run_on_main_engine_ids,
                },
                "data_doc_id": doc_id,
                "task_run_record_id": record_id,
                "deadline_epoch": deadline_epoch,
            }
            tasks_to_run.append(
                _start_query_execution_task.si(
                    **start_query_execution_kwargs,
                    previous_query_result=(QueryExecutionStatus.DONE.value, 0),
                )
                if index == 0
                else _start_query_execution_task.s(**start_query_execution_kwargs)
            )

            run_query_signature = run_query_task.s(execution_type=execution_type)
            if timeout_seconds:
                run_query_signature = run_query_signature.set(
                    soft_time_limit=timeout_seconds,
                    time_limit=timeout_seconds + RUN_QUERY_TIME_LIMIT_BUFFER_SECONDS,
                )
            tasks_to_run.append(run_query_signature)

    chain(*tasks_to_run).apply_async(
        link=on_datadoc_run_success.s(
            completion_params=completion_params,
        ),
        link_error=on_datadoc_run_failure.s(completion_params=completion_params),
    )


@celery.task(bind=True)
def _start_query_execution_task(
    self,
    previous_query_result,
    cell_id,
    query_execution_params,
    data_doc_id,
    task_run_record_id=None,
    deadline_epoch=None,
):
    previous_query_status, previous_query_execution_id = previous_query_result
    if previous_query_status != QueryExecutionStatus.DONE.value:
        raise Exception(get_datadoc_error_message(previous_query_execution_id))

    if deadline_epoch is not None and datetime.datetime.utcnow().timestamp() > deadline_epoch:
        raise DataDocRunTimeoutException(
            f"DataDoc run exceeded timeout before cell {cell_id}"
        )

    with DBSession() as session:
        query_execution = qe_logic.create_query_execution(
            **query_execution_params,
            task_run_record_id=task_run_record_id,
            session=session,
        )
        datadoc_logic.append_query_executions_to_data_cell(
            cell_id,
            [query_execution.id],
            session=session,
        )

        socketio.emit(
            "data_doc_query_execution",
            (
                None,
                query_execution.to_dict(),
                cell_id,
            ),
            namespace="/datadoc",
            room=data_doc_id,
        )
        return query_execution.id


@with_session
def get_datadoc_error_message(query_execution_id, session=None):
    _, data_cell_id = qe_logic.get_datadoc_id_from_query_execution_id(
        query_execution_id, session=session
    )[0]
    data_cell_name = datadoc_logic.get_data_cell_by_id(
        data_cell_id, session=session
    ).meta.get("title", f"Untitled Cell Id [{data_cell_id}]")
    query_execution_error = qe_logic.get_query_execution_error(
        query_execution_id, session=session
    )
    query_execution_error_message = (
        query_execution_error.error_message_extracted
        if query_execution_error.error_message_extracted
        else query_execution_error.error_message
    )
    error_msg = (
        f'Failure in "{data_cell_name}": {query_execution_error_message}'
        if query_execution_error_message is not None
        else GENERIC_QUERY_FAILURE_MSG
    )[:description_length]
    return error_msg


@celery.task
def on_datadoc_run_success(
    last_query_result,
    completion_params,
    **kwargs,
):
    last_query_status, last_query_execution_id = last_query_result

    is_success = last_query_status == QueryExecutionStatus.DONE.value

    deadline_epoch = completion_params.get("deadline_epoch")
    is_timeout = (
        not is_success
        and deadline_epoch is not None
        and datetime.datetime.utcnow().timestamp() > deadline_epoch
    )

    if is_timeout:
        error_msg = (
            "DataDoc run timed out. Cell exceeded the configured timeout."
        )
    elif not is_success:
        error_msg = get_datadoc_error_message(last_query_execution_id)
    else:
        error_msg = None

    return on_datadoc_completion(
        is_success=is_success,
        is_timeout=is_timeout,
        error_msg=error_msg,
        **completion_params,
    )


@celery.task
def on_datadoc_run_failure(
    request,
    exc,
    traceback,
    completion_params,
    **kwargs,
):
    deadline_epoch = completion_params.get("deadline_epoch")
    is_timeout = (
        type(exc).__name__ == "DataDocRunTimeoutException"
        or (
            deadline_epoch is not None
            and datetime.datetime.utcnow().timestamp() > deadline_epoch
        )
    )

    error_msg = (
        "DataDoc run timed out. Task {0!r} exceeded the configured timeout".format(
            request.id
        )
        if is_timeout
        else "DataDoc failed to run. Task {0!r} raised error: {1!r}".format(
            request.id, exc
        )
    )
    return on_datadoc_completion(
        is_success=False,
        is_timeout=is_timeout,
        error_msg=error_msg,
        **completion_params,
    )


def on_datadoc_completion(
    doc_id,
    user_id,
    record_id,
    # Export settings
    exports,
    notifications,
    # Success/Failure handling
    is_success,
    error_msg=None,
    is_timeout=False,
    deadline_epoch=None,
    max_retries=0,
    attempt=1,
    parent_run_record_id=None,
    original_run_kwargs=None,
):
    will_retry = (
        not is_success
        and record_id is not None
        and attempt < max_retries + 1
        and original_run_kwargs is not None
        and _retry_schedule_still_enabled(doc_id)
    )

    try:
        export_urls = []
        if is_success:
            export_urls = export_datadoc(doc_id, user_id, exports)

        if not will_retry:
            notifiy_on_datadoc_complete(
                doc_id,
                is_success,
                notifications,
                error_msg,
                export_urls,
            )

    except Exception as e:
        is_success = False
        error_msg = str(e)
        LOG.error(e, exc_info=True)
        # Export/notify failure after a successful run must not silently
        # bypass retry — re-evaluate eligibility.
        will_retry = (
            not is_success
            and record_id is not None
            and attempt < max_retries + 1
            and original_run_kwargs is not None
            and _retry_schedule_still_enabled(doc_id)
        )
    finally:
        # when record_id is None, it's trigerred by adhoc datadoc run, no need to update the record.
        if record_id:
            if is_success:
                status = TaskRunStatus.SUCCESS
            elif is_timeout:
                status = TaskRunStatus.TIMEOUT
            else:
                status = TaskRunStatus.FAILURE
            update_task_run_record(
                id=record_id,
                status=status,
                error_message=error_msg,
            )
            stats_logger.incr(
                DATADOC_SCHEDULED_RUNS,
                tags={"status": status.name.lower()},
            )

    if will_retry:
        _enqueue_datadoc_retry(
            doc_id=doc_id,
            original_run_kwargs=original_run_kwargs,
            attempt=attempt,
            parent_run_record_id=parent_run_record_id,
        )

    return is_success


def _retry_schedule_still_enabled(doc_id):
    schedule = get_task_schedule_by_name(get_data_doc_schedule_name(doc_id))
    return bool(schedule and schedule.enabled)


def _enqueue_datadoc_retry(
    doc_id, original_run_kwargs, attempt, parent_run_record_id
):
    delay_seconds = min(
        RETRY_BASE_DELAY_SECONDS * (2 ** (attempt - 1)),
        RETRY_MAX_DELAY_SECONDS,
    )
    celery.send_task(
        "tasks.run_datadoc.run_datadoc",
        kwargs={
            **original_run_kwargs,
            "_attempt": attempt + 1,
            "_parent_run_record_id": parent_run_record_id,
        },
        countdown=delay_seconds,
        shadow=get_data_doc_schedule_name(doc_id),
    )
    stats_logger.incr(
        DATADOC_SCHEDULED_RETRIES,
        tags={"attempt": str(attempt + 1)},
    )
