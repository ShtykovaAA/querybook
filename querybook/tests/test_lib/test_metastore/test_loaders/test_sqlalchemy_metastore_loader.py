import unittest
from unittest import TestCase
from unittest.mock import MagicMock, patch, PropertyMock

from const.metastore import DataColumn, DataTable
from lib.metastore.loaders.sqlalchemy_metastore_loader import (
    SqlAlchemyMetastoreLoader,
)


METASTORE_DICT = {
    "id": 1,
    "name": "Test PG Metastore",
    "loader": "SqlAlchemyMetastoreLoader",
    "metastore_params": {
        "connection_string": "postgresql://user:pass@localhost:5432/testdb",
    },
    "acl_control": {},
}


def _make_loader():
    """Create a SqlAlchemyMetastoreLoader with mocked SQLAlchemy internals."""
    with patch.object(
        SqlAlchemyMetastoreLoader, "_get_sqlalchemy"
    ) as mock_get_sa:
        engine = MagicMock()
        engine.dialect.name = "postgresql"
        inspect = MagicMock()
        conn = MagicMock()
        mock_get_sa.return_value = (engine, inspect, conn)

        with patch.object(SqlAlchemyMetastoreLoader, "load", lambda self: None):
            loader = SqlAlchemyMetastoreLoader(METASTORE_DICT)

    return loader


class TestParseIdentityArgs(TestCase):
    """Tests for _parse_identity_args which parses pg_get_function_identity_arguments."""

    def setUp(self):
        self.loader = _make_loader()

    def test_empty_string(self):
        result = self.loader._parse_identity_args("")
        self.assertEqual(result, [])

    def test_none(self):
        result = self.loader._parse_identity_args(None)
        self.assertEqual(result, [])

    def test_whitespace_only(self):
        result = self.loader._parse_identity_args("   ")
        self.assertEqual(result, [])

    def test_simple_params(self):
        result = self.loader._parse_identity_args("x integer, y text")
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0], DataColumn(name="x", type="integer", comment="IN"))
        self.assertEqual(result[1], DataColumn(name="y", type="text", comment="IN"))

    def test_params_with_modes(self):
        result = self.loader._parse_identity_args(
            "IN x integer, OUT result text, INOUT val numeric"
        )
        self.assertEqual(len(result), 3)
        self.assertEqual(result[0], DataColumn(name="x", type="integer", comment="IN"))
        self.assertEqual(
            result[1], DataColumn(name="result", type="text", comment="OUT")
        )
        self.assertEqual(
            result[2], DataColumn(name="val", type="numeric", comment="INOUT")
        )

    def test_variadic_param(self):
        result = self.loader._parse_identity_args("VARIADIC args text[]")
        self.assertEqual(len(result), 1)
        self.assertEqual(
            result[0], DataColumn(name="args", type="text[]", comment="VARIADIC")
        )

    def test_unnamed_param(self):
        """When PostgreSQL has unnamed params, only the type is listed."""
        result = self.loader._parse_identity_args("integer, text")
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0], DataColumn(name="$1", type="integer", comment="IN"))
        self.assertEqual(result[1], DataColumn(name="$2", type="text", comment="IN"))

    def test_compound_types(self):
        result = self.loader._parse_identity_args(
            "x character varying, y double precision"
        )
        self.assertEqual(len(result), 2)
        self.assertEqual(
            result[0],
            DataColumn(name="x", type="character varying", comment="IN"),
        )
        self.assertEqual(
            result[1],
            DataColumn(name="y", type="double precision", comment="IN"),
        )

    def test_single_param(self):
        result = self.loader._parse_identity_args("days_to_keep integer")
        self.assertEqual(len(result), 1)
        self.assertEqual(
            result[0], DataColumn(name="days_to_keep", type="integer", comment="IN")
        )


class TestGetFunctionAndParams(TestCase):
    """Tests for get_function_and_params which converts pg_proc rows to DataTable+DataColumns."""

    def setUp(self):
        self.loader = _make_loader()

    def test_function_basic(self):
        func_info = {
            "routine_name": "get_user_greeting",
            "routine_type": "function",
            "identity_args": "username text, lang text",
            "return_type": "text",
            "routine_language": "plpgsql",
            "routine_definition": "CREATE OR REPLACE FUNCTION ...",
            "owner": "postgres",
        }

        table, columns = self.loader.get_function_and_params("public", func_info)

        self.assertEqual(table.name, "get_user_greeting(username text, lang text)")
        self.assertEqual(table.type, "function")
        self.assertEqual(table.owner, "postgres")
        self.assertEqual(table.raw_description, "CREATE OR REPLACE FUNCTION ...")
        self.assertEqual(table.custom_properties["language"], "plpgsql")
        self.assertEqual(table.custom_properties["return_type"], "text")

        self.assertEqual(len(columns), 2)
        self.assertEqual(columns[0], DataColumn(name="username", type="text", comment="IN"))
        self.assertEqual(columns[1], DataColumn(name="lang", type="text", comment="IN"))

    def test_procedure_basic(self):
        func_info = {
            "routine_name": "cleanup_old_records",
            "routine_type": "procedure",
            "identity_args": "days_to_keep integer",
            "return_type": "",
            "routine_language": "plpgsql",
            "routine_definition": "CREATE OR REPLACE PROCEDURE ...",
            "owner": "admin",
        }

        table, columns = self.loader.get_function_and_params("public", func_info)

        self.assertEqual(table.name, "cleanup_old_records(days_to_keep integer)")
        self.assertEqual(table.type, "procedure")
        self.assertEqual(table.owner, "admin")
        self.assertEqual(table.custom_properties["language"], "plpgsql")

        self.assertEqual(len(columns), 1)
        self.assertEqual(
            columns[0], DataColumn(name="days_to_keep", type="integer", comment="IN")
        )

    def test_function_no_args(self):
        func_info = {
            "routine_name": "now_utc",
            "routine_type": "function",
            "identity_args": "",
            "return_type": "timestamp with time zone",
            "routine_language": "sql",
            "routine_definition": "CREATE OR REPLACE FUNCTION now_utc() ...",
            "owner": "postgres",
        }

        table, columns = self.loader.get_function_and_params("public", func_info)

        # No args → no parentheses appended
        self.assertEqual(table.name, "now_utc")
        self.assertEqual(table.type, "function")
        self.assertEqual(table.custom_properties["return_type"], "timestamp with time zone")
        self.assertEqual(columns, [])

    def test_function_with_out_params(self):
        func_info = {
            "routine_name": "get_stats",
            "routine_type": "function",
            "identity_args": "IN table_name text, OUT row_count bigint, OUT avg_size double precision",
            "return_type": "record",
            "routine_language": "plpgsql",
            "routine_definition": "...",
            "owner": "postgres",
        }

        table, columns = self.loader.get_function_and_params("public", func_info)

        self.assertEqual(
            table.name,
            "get_stats(IN table_name text, OUT row_count bigint, OUT avg_size double precision)",
        )
        self.assertEqual(len(columns), 3)
        self.assertEqual(columns[0].comment, "IN")
        self.assertEqual(columns[1].comment, "OUT")
        self.assertEqual(columns[1].name, "row_count")
        self.assertEqual(columns[2].comment, "OUT")
        self.assertEqual(columns[2].type, "double precision")


class TestGetAllFunctionsInSchema(TestCase):
    """Tests for get_all_functions_in_schema which queries pg_proc."""

    def setUp(self):
        self.loader = _make_loader()

    def test_returns_list_of_dicts(self):
        mock_rows = [
            {
                "routine_name": "func_a",
                "routine_type": "function",
                "identity_args": "x integer",
                "return_type": "integer",
                "routine_language": "plpgsql",
                "routine_definition": "...",
                "owner": "postgres",
            },
            {
                "routine_name": "proc_b",
                "routine_type": "procedure",
                "identity_args": "",
                "return_type": "",
                "routine_language": "plpgsql",
                "routine_definition": "...",
                "owner": "postgres",
            },
        ]
        # Mock conn.execute to return rows that behave like dicts
        mock_result = MagicMock()
        mock_result.__iter__ = MagicMock(return_value=iter(mock_rows))
        self.loader._conn.execute.return_value = mock_result

        result = self.loader.get_all_functions_in_schema("public")

        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["routine_name"], "func_a")
        self.assertEqual(result[0]["routine_type"], "function")
        self.assertEqual(result[1]["routine_name"], "proc_b")
        self.assertEqual(result[1]["routine_type"], "procedure")

    def test_empty_schema(self):
        mock_result = MagicMock()
        mock_result.__iter__ = MagicMock(return_value=iter([]))
        self.loader._conn.execute.return_value = mock_result

        result = self.loader.get_all_functions_in_schema("empty_schema")

        self.assertEqual(result, [])


class TestIsPostgresql(TestCase):
    """Tests for the _is_postgresql property."""

    def test_postgresql_dialect(self):
        loader = _make_loader()
        loader._engine.dialect.name = "postgresql"
        self.assertTrue(loader._is_postgresql)

    def test_mysql_dialect(self):
        loader = _make_loader()
        loader._engine.dialect.name = "mysql"
        self.assertFalse(loader._is_postgresql)

    def test_bigquery_dialect(self):
        loader = _make_loader()
        loader._engine.dialect.name = "bigquery"
        self.assertFalse(loader._is_postgresql)


class TestGetTableAndColumns(TestCase):
    """Tests for get_table_and_columns (existing functionality)."""

    def setUp(self):
        self.loader = _make_loader()

    def test_table_not_found(self):
        self.loader._engine.dialect.has_table.return_value = False

        table, columns = self.loader.get_table_and_columns("public", "nonexistent")

        self.assertIsNone(table)
        self.assertEqual(columns, [])

    def test_table_found(self):
        self.loader._engine.dialect.has_table.return_value = True
        self.loader._inspect.get_columns.return_value = [
            {"name": "id", "type": "INTEGER", "default": None, "nullable": False},
            {"name": "name", "type": "VARCHAR", "default": None, "nullable": True},
        ]

        table, columns = self.loader.get_table_and_columns("public", "users")

        self.assertEqual(table.name, "users")
        self.assertIsNone(table.type)
        self.assertEqual(len(columns), 2)
        self.assertEqual(columns[0].name, "id")
        self.assertEqual(columns[0].type, "INTEGER")
        self.assertEqual(columns[1].name, "name")
