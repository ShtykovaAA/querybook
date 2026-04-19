from unittest import TestCase, mock
from lib.scheduled_datadoc.validator import (
    validate_dict_keys,
    validate_exporters_config,
    validate_datadoc_schedule_config,
    validate_timeout_and_retries,
    InvalidScheduleException,
)


class ValidateDictKeysTestCase(TestCase):
    test_dict = {"foo": "bar", "hello": "world"}

    def test_valid_dict(self):
        validate_dict_keys(self.test_dict, ["foo", "hello"])

    def test_valid_dict_with_extra_keys(self):
        validate_dict_keys(self.test_dict, ["foo", "hello", "bar"])

    def test_invalid_dict(self):
        with self.assertRaises(InvalidScheduleException):
            validate_dict_keys(self.test_dict, ["hello", "bar"])


def mock_get_exporter(name: str):
    if name != "export_to_table":
        raise ValueError("Invalid exporter")
    exporter = mock.MagicMock()
    exporter.export_form = None
    return exporter


class ValidateExportersConfigTestCase(TestCase):
    def setUp(self):
        patch_get_exporter = mock.patch(
            "lib.scheduled_datadoc.validator.get_exporter",
            side_effect=mock_get_exporter,
        )
        self.addCleanup(patch_get_exporter.stop)
        self.mock_get_exporter = patch_get_exporter.start()

    @mock.patch(
        "lib.scheduled_datadoc.validator.validate_form", return_value=(True, "")
    )
    def test_valid_config(self, mock_get_exporter):
        validate_exporters_config(
            [
                {
                    "exporter_cell_id": 1,
                    "exporter_name": "export_to_table",
                    "exporter_params": {"table": "a"},
                },
                {
                    "exporter_cell_id": 2,
                    "exporter_name": "export_to_table",
                    "exporter_params": {"table": "b"},
                },
            ]
        )

    def test_missing_exporter_cell_id(self):
        with self.assertRaises(InvalidScheduleException):
            validate_exporters_config(
                [{"exporter_name": "not_exists", "exporter_params": {"table": "a"}}]
            )

    def test_invalid_exporter_name(self):
        with self.assertRaises(InvalidScheduleException):
            validate_exporters_config(
                [
                    {
                        "exporter_cell_id": 1,
                        "exporter_name": "not_exists",
                        "exporter_params": {"table": "a"},
                    }
                ]
            )

    @mock.patch(
        "lib.scheduled_datadoc.validator.validate_form", return_value=(False, "Invalid")
    )
    def test_invalid_exporter_form(self, mock_validate_form):
        with self.assertRaises(InvalidScheduleException):
            validate_exporters_config(
                [
                    {
                        "exporter_cell_id": 1,
                        "exporter_name": "export_to_table",
                        "exporter_params": {"table": "a"},
                    }
                ]
            )


class ValidateDatadocScheduleConfigTestCase(TestCase):
    @mock.patch("lib.scheduled_datadoc.validator.validate_dict_keys")
    def test_invalid_dict(self, mock_validate_dict_keys):
        mock_validate_dict_keys.side_effect = InvalidScheduleException()
        self.assertFalse(validate_datadoc_schedule_config({})[0])

    @mock.patch("lib.scheduled_datadoc.validator.validate_dict_keys")
    @mock.patch("lib.scheduled_datadoc.validator.validate_exporters_config")
    def test_invalid_exporter(
        self, mock_validate_dict_keys, mock_validate_exporters_config
    ):
        mock_validate_exporters_config.side_effect = InvalidScheduleException()
        self.assertFalse(validate_datadoc_schedule_config({})[0])
        self.assertTrue(mock_validate_exporters_config.called)

    def test_valid_timeout_and_retries(self):
        ok, reason = validate_datadoc_schedule_config(
            {"timeout_seconds": 3600, "max_retries": 3}
        )
        self.assertTrue(ok)
        self.assertEqual(reason, "")

    def test_missing_timeout_and_retries_is_valid(self):
        ok, _ = validate_datadoc_schedule_config({})
        self.assertTrue(ok)

    def test_invalid_timeout_too_low(self):
        ok, _ = validate_datadoc_schedule_config({"timeout_seconds": 30})
        self.assertFalse(ok)

    def test_invalid_timeout_too_high(self):
        ok, _ = validate_datadoc_schedule_config({"timeout_seconds": 200_000})
        self.assertFalse(ok)

    def test_invalid_timeout_wrong_type(self):
        ok, _ = validate_datadoc_schedule_config({"timeout_seconds": "abc"})
        self.assertFalse(ok)

    def test_invalid_timeout_bool_rejected(self):
        ok, _ = validate_datadoc_schedule_config({"timeout_seconds": True})
        self.assertFalse(ok)

    def test_invalid_max_retries_negative(self):
        ok, _ = validate_datadoc_schedule_config({"max_retries": -1})
        self.assertFalse(ok)

    def test_invalid_max_retries_above_limit(self):
        ok, _ = validate_datadoc_schedule_config({"max_retries": 11})
        self.assertFalse(ok)

    def test_invalid_max_retries_wrong_type(self):
        ok, _ = validate_datadoc_schedule_config({"max_retries": "foo"})
        self.assertFalse(ok)


class ValidateTimeoutAndRetriesTestCase(TestCase):
    def test_none_values_are_allowed(self):
        validate_timeout_and_retries(
            {"timeout_seconds": None, "max_retries": None}
        )

    def test_boundary_timeout_values(self):
        validate_timeout_and_retries({"timeout_seconds": 60})
        validate_timeout_and_retries({"timeout_seconds": 172_800})

    def test_boundary_max_retries_values(self):
        validate_timeout_and_retries({"max_retries": 0})
        validate_timeout_and_retries({"max_retries": 10})
