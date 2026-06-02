from typing import Dict, List

from const.schedule import NotifyOn
from lib.export.all_exporters import get_exporter
from lib.notify.all_notifiers import get_notifier_class
from lib.form import validate_form


class InvalidScheduleException(Exception):
    pass


valid_schedule_config_keys = [
    "exports",
    "notifications",
    "timeout_seconds",
    "max_retries",
    "run_on_main_engine_ids",
]
valid_export_config_keys = ["exporter_cell_id", "exporter_name", "exporter_params"]
valid_notification_keys = ["with", "on", "config"]
valid_notification_config_keys = ["to", "to_user"]

MIN_TIMEOUT_SECONDS = 60
MAX_TIMEOUT_SECONDS = 172_800  # 2 days — matches Celery task_soft_time_limit
MAX_RETRIES_LIMIT = 10


def validate_datadoc_schedule_config(schedule_config):
    try:
        validate_dict_keys(schedule_config, valid_schedule_config_keys)
        validate_notifications_config(schedule_config.get("notifications", []))
        validate_exporters_config(schedule_config.get("exports", []))
        validate_timeout_and_retries(schedule_config)
        validate_run_on_main_engine_ids(schedule_config)
    except InvalidScheduleException as e:
        return False, str(e)
    return True, ""


def validate_run_on_main_engine_ids(schedule_config):
    """Shape-only check. Cross-validation against the DataDoc's actual cell
    engines and each engine's main_connection_string happens at the
    endpoint layer (see datasources/datadoc.py)."""
    if "run_on_main_engine_ids" not in schedule_config:
        return
    value = schedule_config["run_on_main_engine_ids"]
    if value is None:
        return
    if not isinstance(value, list):
        raise InvalidScheduleException("run_on_main_engine_ids must be a list")
    for item in value:
        if not isinstance(item, int) or isinstance(item, bool):
            raise InvalidScheduleException(
                "run_on_main_engine_ids must contain integer engine ids"
            )


def validate_timeout_and_retries(schedule_config):
    if "timeout_seconds" in schedule_config:
        timeout = schedule_config["timeout_seconds"]
        if timeout is not None and (
            not isinstance(timeout, int)
            or isinstance(timeout, bool)
            or timeout < MIN_TIMEOUT_SECONDS
            or timeout > MAX_TIMEOUT_SECONDS
        ):
            raise InvalidScheduleException(
                f"timeout_seconds must be an integer in "
                f"[{MIN_TIMEOUT_SECONDS}, {MAX_TIMEOUT_SECONDS}]"
            )

    if "max_retries" in schedule_config:
        retries = schedule_config["max_retries"]
        if retries is not None and (
            not isinstance(retries, int)
            or isinstance(retries, bool)
            or retries < 0
            or retries > MAX_RETRIES_LIMIT
        ):
            raise InvalidScheduleException(
                f"max_retries must be an integer in [0, {MAX_RETRIES_LIMIT}]"
            )


def validate_dict_keys(d: Dict, allowed_keys: List):
    for key in d.keys():
        if key not in allowed_keys:
            raise InvalidScheduleException(f"Invalid field {key}")


def validate_notifications_config(notifications: List):
    if not notifications:
        return

    for notification in notifications:
        validate_dict_keys(notification, valid_notification_keys)
        validate_dict_keys(notification.get("config"), valid_notification_config_keys)

        # validate notify with
        notifier_name = notification.get("with", None)
        try:
            get_notifier_class(notifier_name)
        except ValueError:
            raise InvalidScheduleException(f"Invalid notifier {notifier_name}")

        # validate notify on
        if notification.get("on") not in [on.value for on in NotifyOn]:
            raise InvalidScheduleException(
                f"Invalid notify on {notification.get('on')}"
            )


def validate_exporters_config(export_configs: List):
    if not export_configs:
        return

    for export_config in export_configs:
        validate_dict_keys(export_config, valid_export_config_keys)

        if export_config.get("exporter_cell_id", None) is None:
            raise InvalidScheduleException("exporter_cell_id is required")

        exporter = _get_exporter(export_config)
        exporter_params = export_config.get("exporter_params", {})
        exporter_form = exporter.export_form
        if exporter_form is not None or exporter_params:
            valid, reason = validate_form(exporter_form, exporter_params)
            if not valid:
                raise InvalidScheduleException(
                    f"Invalid exporter params, reason: {reason}"
                )


def _get_exporter(export_config):
    exporter_name = export_config.get("exporter_name", None)
    try:
        return get_exporter(exporter_name)
    except ValueError:
        raise InvalidScheduleException(f"Invalid exporter {exporter_name}")
