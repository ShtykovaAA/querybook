import unittest
from unittest import TestCase
from unittest.mock import MagicMock, call, patch, PropertyMock

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


class TestLoadFunctionsSameNameDifferentTypes(TestCase):
    """Test that a function and procedure with the same base name
    are both loaded and deleted independently."""

    def setUp(self):
        self.loader = _make_loader()

    def _make_func_infos(self, *items):
        """Build list of pg_proc-style rows.
        Each item is (name, type, identity_args).
        """
        return [
            {
                "routine_name": name,
                "routine_type": rtype,
                "identity_args": args,
                "return_type": "void" if rtype == "procedure" else "integer",
                "routine_language": "plpgsql",
                "routine_definition": f"CREATE OR REPLACE {rtype.upper()} {name}...",
                "owner": "postgres",
            }
            for name, rtype, args in items
        ]

    @patch(
        "lib.metastore.loaders.sqlalchemy_metastore_loader.SqlAlchemyMetastoreLoader._create_table_table"
    )
    @patch("lib.metastore.base_metastore_loader.get_table_by_schema_id")
    @patch("logic.metastore.get_schema_by_name")
    @patch("app.db.DBSession")
    def test_same_name_function_and_procedure_both_created(
        self, mock_db_session, mock_get_schema, mock_get_by_schema, mock_create_table
    ):
        """A function process_data(x int) and procedure process_data()
        should both be created — they have different display names."""
        mock_schema = MagicMock()
        mock_schema.id = 42
        mock_get_schema.return_value = mock_schema
        mock_db_session.return_value.__enter__ = MagicMock(return_value=MagicMock())
        mock_db_session.return_value.__exit__ = MagicMock(return_value=False)

        # No existing objects in DB — nothing to delete
        mock_get_by_schema.return_value = []

        func_infos = self._make_func_infos(
            ("process_data", "function", "x integer"),
            ("process_data", "procedure", ""),
        )

        mock_result = MagicMock()
        mock_result.__iter__ = MagicMock(return_value=iter(func_infos))
        self.loader._conn.execute.return_value = mock_result
        self.loader._get_all_filtered_schema_names = MagicMock(
            return_value=["public"]
        )

        self.loader._load_functions()

        # Both should be created
        self.assertEqual(mock_create_table.call_count, 2)

        created_names = {
            c.kwargs.get("table", c.args[3] if len(c.args) > 3 else None).name
            if c.kwargs.get("table")
            else c[1][2]
            for c in mock_create_table.call_args_list
        }
        # function gets display name with args, procedure has no args
        self.assertIn("process_data(x integer)", created_names)
        self.assertIn("process_data", created_names)

    @patch(
        "lib.metastore.loaders.sqlalchemy_metastore_loader.SqlAlchemyMetastoreLoader._create_table_table"
    )
    @patch("lib.metastore.base_metastore_loader.get_table_by_schema_id")
    @patch("logic.metastore.get_schema_by_name")
    @patch("app.db.DBSession")
    def test_delete_only_removes_matching_type(
        self, mock_db_session, mock_get_schema, mock_get_by_schema, mock_create_table
    ):
        """When a procedure is removed from PG but the function remains,
        only the procedure should be deleted from metastore."""
        mock_schema = MagicMock()
        mock_schema.id = 42
        mock_get_schema.return_value = mock_schema
        mock_db_session.return_value.__enter__ = MagicMock(return_value=MagicMock())
        mock_db_session.return_value.__exit__ = MagicMock(return_value=False)

        # Existing objects in DB: function + procedure with same base name
        existing_func = MagicMock()
        existing_func.id = 100
        existing_func.name = "process_data(x integer)"
        existing_func.type = "function"

        existing_proc = MagicMock()
        existing_proc.id = 101
        existing_proc.name = "process_data"
        existing_proc.type = "procedure"

        # get_table_by_schema_id is called twice:
        # 1st with table_type="function" → returns [existing_func]
        # 2nd with table_type="procedure" → returns [existing_proc]
        mock_get_by_schema.side_effect = [
            [existing_func],  # functions
            [existing_proc],  # procedures
        ]

        # Only the function remains in PG, procedure was dropped
        func_infos = self._make_func_infos(
            ("process_data", "function", "x integer"),
        )

        mock_result = MagicMock()
        mock_result.__iter__ = MagicMock(return_value=iter(func_infos))
        self.loader._conn.execute.return_value = mock_result
        self.loader._get_all_filtered_schema_names = MagicMock(
            return_value=["public"]
        )

        with patch(
            "lib.metastore.base_metastore_loader.delete_table"
        ) as mock_delete, patch(
            "lib.metastore.base_metastore_loader.delete_es_table_by_id"
        ):
            self.loader._load_functions()

            # Only procedure (id=101) should be deleted, function stays
            mock_delete.assert_called_once()
            deleted_id = mock_delete.call_args[1]["table_id"]
            self.assertEqual(deleted_id, 101)

    @patch(
        "lib.metastore.loaders.sqlalchemy_metastore_loader.SqlAlchemyMetastoreLoader._create_table_table"
    )
    @patch("lib.metastore.base_metastore_loader.get_table_by_schema_id")
    @patch("logic.metastore.get_schema_by_name")
    @patch("app.db.DBSession")
    def test_table_not_deleted_when_functions_loaded(
        self, mock_db_session, mock_get_schema, mock_get_by_schema, mock_create_table
    ):
        """Regular tables (type=None) must not be deleted by _load_functions."""
        mock_schema = MagicMock()
        mock_schema.id = 42
        mock_get_schema.return_value = mock_schema
        mock_db_session.return_value.__enter__ = MagicMock(return_value=MagicMock())
        mock_db_session.return_value.__exit__ = MagicMock(return_value=False)

        # get_table_by_schema_id with type="function" and type="procedure"
        # should NOT return regular tables — they have type=None
        mock_get_by_schema.return_value = []

        func_infos = self._make_func_infos(
            ("my_func", "function", "x integer"),
        )

        mock_result = MagicMock()
        mock_result.__iter__ = MagicMock(return_value=iter(func_infos))
        self.loader._conn.execute.return_value = mock_result
        self.loader._get_all_filtered_schema_names = MagicMock(
            return_value=["public"]
        )

        with patch(
            "lib.metastore.base_metastore_loader.delete_table"
        ) as mock_delete, patch(
            "lib.metastore.base_metastore_loader.delete_es_table_by_id"
        ):
            self.loader._load_functions()

            # No tables should be deleted
            mock_delete.assert_not_called()

        # get_table_by_schema_id should be called with specific types
        for c in mock_get_by_schema.call_args_list:
            self.assertIn(
                c.kwargs.get("table_type", c[1] if len(c.args) > 1 else None),
                ["function", "procedure"],
            )
