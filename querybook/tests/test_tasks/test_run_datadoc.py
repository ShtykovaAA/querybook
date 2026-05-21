import datetime
from unittest import TestCase, mock

from const.query_execution import QueryExecutionStatus
from const.schedule import TaskRunStatus
from lib.scheduled_datadoc.exc import DataDocRunTimeoutException
from tasks.run_datadoc import (
    _start_query_execution_task,
    on_datadoc_completion,
    on_datadoc_run_failure,
)


def _past_epoch(seconds_ago=60):
    return (
        datetime.datetime.utcnow() - datetime.timedelta(seconds=seconds_ago)
    ).timestamp()


def _future_epoch(seconds_ahead=3600):
    return (
        datetime.datetime.utcnow() + datetime.timedelta(seconds=seconds_ahead)
    ).timestamp()


class StartQueryExecutionTaskDeadlineTestCase(TestCase):
    def test_expired_deadline_raises_timeout(self):
        with self.assertRaises(DataDocRunTimeoutException):
            _start_query_execution_task.run(
                previous_query_result=(QueryExecutionStatus.DONE.value, 0),
                cell_id=1,
                query_execution_params={"query": "SELECT 1", "engine_id": 1, "uid": 1},
                data_doc_id=10,
                task_run_record_id=None,
                deadline_epoch=_past_epoch(),
            )

    @mock.patch("tasks.run_datadoc.socketio")
    @mock.patch("tasks.run_datadoc.datadoc_logic")
    @mock.patch("tasks.run_datadoc.qe_logic")
    @mock.patch("tasks.run_datadoc.DBSession")
    def test_future_deadline_proceeds(
        self, mock_dbsession, mock_qe_logic, mock_datadoc_logic, mock_socketio
    ):
        mock_dbsession.return_value.__enter__.return_value = mock.MagicMock()
        qe = mock.MagicMock()
        qe.id = 42
        qe.to_dict.return_value = {"id": 42}
        mock_qe_logic.create_query_execution.return_value = qe

        result = _start_query_execution_task.run(
            previous_query_result=(QueryExecutionStatus.DONE.value, 0),
            cell_id=1,
            query_execution_params={"query": "SELECT 1", "engine_id": 1, "uid": 1},
            data_doc_id=10,
            task_run_record_id=7,
            deadline_epoch=_future_epoch(),
        )
        self.assertEqual(result, 42)
        mock_qe_logic.create_query_execution.assert_called_once()

    @mock.patch("tasks.run_datadoc.socketio")
    @mock.patch("tasks.run_datadoc.datadoc_logic")
    @mock.patch("tasks.run_datadoc.qe_logic")
    @mock.patch("tasks.run_datadoc.DBSession")
    def test_no_deadline_proceeds(
        self, mock_dbsession, mock_qe_logic, mock_datadoc_logic, mock_socketio
    ):
        mock_dbsession.return_value.__enter__.return_value = mock.MagicMock()
        qe = mock.MagicMock()
        qe.id = 42
        qe.to_dict.return_value = {"id": 42}
        mock_qe_logic.create_query_execution.return_value = qe

        result = _start_query_execution_task.run(
            previous_query_result=(QueryExecutionStatus.DONE.value, 0),
            cell_id=1,
            query_execution_params={"query": "SELECT 1", "engine_id": 1, "uid": 1},
            data_doc_id=10,
        )
        self.assertEqual(result, 42)


class OnDatadocCompletionStatusTestCase(TestCase):
    def _base_params(self):
        return {
            "doc_id": 1,
            "user_id": 1,
            "record_id": 99,
            "exports": [],
            "notifications": [],
        }

    @mock.patch("tasks.run_datadoc.update_task_run_record")
    @mock.patch("tasks.run_datadoc.notifiy_on_datadoc_complete")
    @mock.patch("tasks.run_datadoc.export_datadoc", return_value=[])
    def test_success_sets_success_status(self, _exp, _notify, mock_update):
        on_datadoc_completion(is_success=True, **self._base_params())
        self.assertEqual(mock_update.call_args.kwargs["status"], TaskRunStatus.SUCCESS)

    @mock.patch("tasks.run_datadoc.update_task_run_record")
    @mock.patch("tasks.run_datadoc.notifiy_on_datadoc_complete")
    def test_failure_sets_failure_status(self, _notify, mock_update):
        on_datadoc_completion(
            is_success=False, error_msg="boom", **self._base_params()
        )
        self.assertEqual(mock_update.call_args.kwargs["status"], TaskRunStatus.FAILURE)

    @mock.patch("tasks.run_datadoc.update_task_run_record")
    @mock.patch("tasks.run_datadoc.notifiy_on_datadoc_complete")
    def test_timeout_sets_timeout_status(self, _notify, mock_update):
        on_datadoc_completion(
            is_success=False,
            is_timeout=True,
            error_msg="timed out",
            **self._base_params(),
        )
        self.assertEqual(mock_update.call_args.kwargs["status"], TaskRunStatus.TIMEOUT)

    @mock.patch("tasks.run_datadoc.update_task_run_record")
    @mock.patch("tasks.run_datadoc.notifiy_on_datadoc_complete")
    def test_no_record_id_skips_db_update(self, _notify, mock_update):
        params = self._base_params()
        params["record_id"] = None
        on_datadoc_completion(is_success=True, **params)
        mock_update.assert_not_called()


class OnDatadocCompletionRetryTestCase(TestCase):
    def _base_params(self, **overrides):
        params = {
            "doc_id": 1,
            "user_id": 1,
            "record_id": 99,
            "exports": [],
            "notifications": [],
            "max_retries": 2,
            "attempt": 1,
            "parent_run_record_id": 99,
            "original_run_kwargs": {
                "doc_id": 1,
                "user_id": 1,
                "execution_type": "scheduled",
                "notifications": [],
                "exports": [],
                "max_retries": 2,
            },
        }
        params.update(overrides)
        return params

    @mock.patch("tasks.run_datadoc.celery")
    @mock.patch("tasks.run_datadoc.get_task_schedule_by_name")
    @mock.patch("tasks.run_datadoc.update_task_run_record")
    @mock.patch("tasks.run_datadoc.notifiy_on_datadoc_complete")
    def test_failure_under_budget_enqueues_retry(
        self, mock_notify, _upd, mock_get_schedule, mock_celery
    ):
        mock_get_schedule.return_value = mock.MagicMock(enabled=True)
        on_datadoc_completion(
            is_success=False, error_msg="boom", **self._base_params()
        )
        mock_celery.send_task.assert_called_once()
        call_kwargs = mock_celery.send_task.call_args.kwargs
        self.assertEqual(
            call_kwargs["kwargs"]["_attempt"], 2
        )
        self.assertEqual(call_kwargs["kwargs"]["_parent_run_record_id"], 99)
        self.assertEqual(call_kwargs["countdown"], 60)
        self.assertEqual(call_kwargs["shadow"], "run_data_doc_1")
        # Intermediate attempt: no notification
        mock_notify.assert_not_called()

    @mock.patch("tasks.run_datadoc.celery")
    @mock.patch("tasks.run_datadoc.get_task_schedule_by_name")
    @mock.patch("tasks.run_datadoc.update_task_run_record")
    @mock.patch("tasks.run_datadoc.notifiy_on_datadoc_complete")
    def test_backoff_grows_exponentially(
        self, _notify, _upd, mock_get_schedule, mock_celery
    ):
        mock_get_schedule.return_value = mock.MagicMock(enabled=True)
        on_datadoc_completion(
            is_success=False,
            error_msg="boom",
            **self._base_params(attempt=3, max_retries=6),
        )
        self.assertEqual(
            mock_celery.send_task.call_args.kwargs["countdown"], 60 * 4
        )

    @mock.patch("tasks.run_datadoc.celery")
    @mock.patch("tasks.run_datadoc.get_task_schedule_by_name")
    @mock.patch("tasks.run_datadoc.update_task_run_record")
    @mock.patch("tasks.run_datadoc.notifiy_on_datadoc_complete")
    def test_backoff_capped_at_half_hour(
        self, _notify, _upd, mock_get_schedule, mock_celery
    ):
        mock_get_schedule.return_value = mock.MagicMock(enabled=True)
        on_datadoc_completion(
            is_success=False,
            error_msg="boom",
            **self._base_params(attempt=10, max_retries=10),
        )
        self.assertEqual(
            mock_celery.send_task.call_args.kwargs["countdown"], 30 * 60
        )

    @mock.patch("tasks.run_datadoc.celery")
    @mock.patch("tasks.run_datadoc.get_task_schedule_by_name")
    @mock.patch("tasks.run_datadoc.update_task_run_record")
    @mock.patch("tasks.run_datadoc.notifiy_on_datadoc_complete")
    def test_final_attempt_does_not_retry_and_notifies(
        self, mock_notify, _upd, mock_get_schedule, mock_celery
    ):
        mock_get_schedule.return_value = mock.MagicMock(enabled=True)
        on_datadoc_completion(
            is_success=False,
            error_msg="boom",
            **self._base_params(attempt=3, max_retries=2),
        )
        mock_celery.send_task.assert_not_called()
        mock_notify.assert_called_once()

    @mock.patch("tasks.run_datadoc.celery")
    @mock.patch("tasks.run_datadoc.get_task_schedule_by_name")
    @mock.patch("tasks.run_datadoc.update_task_run_record")
    @mock.patch("tasks.run_datadoc.notifiy_on_datadoc_complete")
    def test_max_retries_zero_does_not_retry(
        self, mock_notify, _upd, mock_get_schedule, mock_celery
    ):
        mock_get_schedule.return_value = mock.MagicMock(enabled=True)
        on_datadoc_completion(
            is_success=False,
            error_msg="boom",
            **self._base_params(max_retries=0),
        )
        mock_celery.send_task.assert_not_called()
        mock_notify.assert_called_once()

    @mock.patch("tasks.run_datadoc.celery")
    @mock.patch("tasks.run_datadoc.get_task_schedule_by_name")
    @mock.patch("tasks.run_datadoc.update_task_run_record")
    @mock.patch("tasks.run_datadoc.notifiy_on_datadoc_complete")
    def test_success_does_not_retry_and_notifies(
        self, mock_notify, _upd, mock_get_schedule, mock_celery
    ):
        mock_get_schedule.return_value = mock.MagicMock(enabled=True)
        on_datadoc_completion(is_success=True, **self._base_params())
        mock_celery.send_task.assert_not_called()
        mock_notify.assert_called_once()

    @mock.patch("tasks.run_datadoc.celery")
    @mock.patch("tasks.run_datadoc.get_task_schedule_by_name")
    @mock.patch("tasks.run_datadoc.update_task_run_record")
    @mock.patch("tasks.run_datadoc.notifiy_on_datadoc_complete")
    def test_disabled_schedule_skips_retry_and_notifies(
        self, mock_notify, _upd, mock_get_schedule, mock_celery
    ):
        mock_get_schedule.return_value = mock.MagicMock(enabled=False)
        on_datadoc_completion(
            is_success=False, error_msg="boom", **self._base_params()
        )
        mock_celery.send_task.assert_not_called()
        # When no retry will happen, user should see the failure.
        mock_notify.assert_called_once()

    @mock.patch("tasks.run_datadoc.celery")
    @mock.patch("tasks.run_datadoc.update_task_run_record")
    @mock.patch("tasks.run_datadoc.notifiy_on_datadoc_complete")
    def test_no_record_id_skips_retry(self, _notify, _upd, mock_celery):
        params = self._base_params()
        params["record_id"] = None
        on_datadoc_completion(is_success=False, error_msg="boom", **params)
        mock_celery.send_task.assert_not_called()


class StatsLoggerEmissionTestCase(TestCase):
    def _base_params(self, **overrides):
        params = {
            "doc_id": 1,
            "user_id": 1,
            "record_id": 99,
            "exports": [],
            "notifications": [],
            "max_retries": 0,
            "attempt": 1,
            "parent_run_record_id": 99,
            "original_run_kwargs": None,
        }
        params.update(overrides)
        return params

    @mock.patch("tasks.run_datadoc.stats_logger")
    @mock.patch("tasks.run_datadoc.update_task_run_record")
    @mock.patch("tasks.run_datadoc.notifiy_on_datadoc_complete")
    @mock.patch("tasks.run_datadoc.export_datadoc", return_value=[])
    def test_success_run_metric(self, _exp, _notify, _upd, mock_stats):
        on_datadoc_completion(is_success=True, **self._base_params())
        mock_stats.incr.assert_any_call(
            "datadoc_scheduled_runs", tags={"status": "success"}
        )

    @mock.patch("tasks.run_datadoc.stats_logger")
    @mock.patch("tasks.run_datadoc.update_task_run_record")
    @mock.patch("tasks.run_datadoc.notifiy_on_datadoc_complete")
    def test_failure_run_metric(self, _notify, _upd, mock_stats):
        on_datadoc_completion(
            is_success=False, error_msg="boom", **self._base_params()
        )
        mock_stats.incr.assert_any_call(
            "datadoc_scheduled_runs", tags={"status": "failure"}
        )

    @mock.patch("tasks.run_datadoc.stats_logger")
    @mock.patch("tasks.run_datadoc.update_task_run_record")
    @mock.patch("tasks.run_datadoc.notifiy_on_datadoc_complete")
    def test_timeout_run_metric(self, _notify, _upd, mock_stats):
        on_datadoc_completion(
            is_success=False,
            is_timeout=True,
            error_msg="timed out",
            **self._base_params(),
        )
        mock_stats.incr.assert_any_call(
            "datadoc_scheduled_runs", tags={"status": "timeout"}
        )

    @mock.patch("tasks.run_datadoc.stats_logger")
    @mock.patch("tasks.run_datadoc.update_task_run_record")
    @mock.patch("tasks.run_datadoc.notifiy_on_datadoc_complete")
    def test_no_record_id_skips_run_metric(self, _notify, _upd, mock_stats):
        params = self._base_params()
        params["record_id"] = None
        on_datadoc_completion(is_success=True, **params)
        mock_stats.incr.assert_not_called()

    @mock.patch("tasks.run_datadoc.stats_logger")
    @mock.patch("tasks.run_datadoc.celery")
    @mock.patch("tasks.run_datadoc.get_task_schedule_by_name")
    @mock.patch("tasks.run_datadoc.update_task_run_record")
    @mock.patch("tasks.run_datadoc.notifiy_on_datadoc_complete")
    def test_retry_metric_emitted(
        self, _notify, _upd, mock_get_schedule, _celery, mock_stats
    ):
        mock_get_schedule.return_value = mock.MagicMock(enabled=True)
        on_datadoc_completion(
            is_success=False,
            error_msg="boom",
            **self._base_params(
                max_retries=2,
                attempt=1,
                original_run_kwargs={
                    "doc_id": 1,
                    "user_id": 1,
                    "execution_type": "scheduled",
                    "notifications": [],
                    "exports": [],
                    "max_retries": 2,
                },
            ),
        )
        mock_stats.incr.assert_any_call(
            "datadoc_scheduled_retries", tags={"attempt": "2"}
        )


class OnDatadocRunFailureClassificationTestCase(TestCase):
    def _base_completion_params(self, deadline_epoch=None):
        return {
            "doc_id": 1,
            "user_id": 1,
            "record_id": 99,
            "exports": [],
            "notifications": [],
            "deadline_epoch": deadline_epoch,
        }

    @mock.patch("tasks.run_datadoc.on_datadoc_completion")
    def test_timeout_exception_classified_as_timeout(self, mock_completion):
        request = mock.MagicMock()
        request.id = "req-1"
        exc = DataDocRunTimeoutException("too slow")
        on_datadoc_run_failure.run(
            request, exc, "trace", self._base_completion_params()
        )
        self.assertTrue(mock_completion.call_args.kwargs["is_timeout"])

    @mock.patch("tasks.run_datadoc.on_datadoc_completion")
    def test_expired_deadline_classified_as_timeout(self, mock_completion):
        request = mock.MagicMock()
        request.id = "req-1"
        on_datadoc_run_failure.run(
            request,
            RuntimeError("other error"),
            "trace",
            self._base_completion_params(deadline_epoch=_past_epoch()),
        )
        self.assertTrue(mock_completion.call_args.kwargs["is_timeout"])

    @mock.patch("tasks.run_datadoc.on_datadoc_completion")
    def test_unrelated_failure_not_classified_as_timeout(self, mock_completion):
        request = mock.MagicMock()
        request.id = "req-1"
        on_datadoc_run_failure.run(
            request,
            RuntimeError("other error"),
            "trace",
            self._base_completion_params(deadline_epoch=_future_epoch()),
        )
        self.assertFalse(mock_completion.call_args.kwargs["is_timeout"])

    @mock.patch("tasks.run_datadoc.on_datadoc_completion")
    def test_no_deadline_not_classified_as_timeout(self, mock_completion):
        request = mock.MagicMock()
        request.id = "req-1"
        on_datadoc_run_failure.run(
            request, RuntimeError("other"), "trace", self._base_completion_params()
        )
        self.assertFalse(mock_completion.call_args.kwargs["is_timeout"])
