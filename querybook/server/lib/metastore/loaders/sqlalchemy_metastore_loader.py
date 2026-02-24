from typing import Dict, List, Tuple

from const.metastore import DataColumn, DataTable
from lib.logger import get_logger
from lib.metastore.base_metastore_loader import BaseMetastoreLoader
from lib.query_executor.connection_string.sqlalchemy import create_sqlalchemy_engine
from lib.query_executor.executor_template.templates import sqlalchemy_template
from sqlalchemy import text

LOG = get_logger(__name__)

_PG_FUNCTIONS_QUERY = text("""
    SELECT
        p.proname AS routine_name,
        CASE p.prokind
            WHEN 'f' THEN 'function'
            WHEN 'p' THEN 'procedure'
        END AS routine_type,
        pg_catalog.pg_get_function_identity_arguments(p.oid) AS identity_args,
        pg_catalog.pg_get_function_result(p.oid) AS return_type,
        l.lanname AS routine_language,
        pg_catalog.pg_get_functiondef(p.oid) AS routine_definition,
        pg_catalog.pg_get_userbyid(p.proowner) AS owner
    FROM pg_catalog.pg_proc p
    JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
    JOIN pg_catalog.pg_language l ON l.oid = p.prolang
    WHERE n.nspname = :schema_name
      AND p.prokind IN ('f', 'p')
      AND l.lanname != 'internal'
      AND l.lanname != 'c'
""")

_PG_FUNCTION_PARAMS_QUERY = text("""
    SELECT
        p.parameter_name,
        p.data_type,
        p.parameter_mode
    FROM information_schema.parameters p
    WHERE p.specific_schema = :schema_name
      AND p.specific_name = :specific_name
    ORDER BY p.ordinal_position
""")


class SqlAlchemyMetastoreLoader(BaseMetastoreLoader):
    def __init__(self, metastore_dict: Dict):
        self._engine, self._inspect, self._conn = self._get_sqlalchemy(metastore_dict)
        super(SqlAlchemyMetastoreLoader, self).__init__(metastore_dict)

    def __del__(self):
        self._conn.close()
        del self._inspect
        self._engine.dispose()

    @classmethod
    def get_metastore_params_template(cls):
        return sqlalchemy_template

    def get_all_schema_names(self) -> List[str]:
        return self._inspect.get_schema_names()

    def get_all_table_names_in_schema(self, schema_name: str) -> List[str]:
        if self._engine.dialect.name == "bigquery":
            return [
                table.split(".")[1]
                for table in self._inspect.get_table_names(schema=schema_name)
            ]
        else:
            return self._inspect.get_table_names(schema=schema_name)

    def get_table_and_columns(
        self, schema_name, table_name
    ) -> Tuple[DataTable, List[DataColumn]]:
        if not self._engine.dialect.has_table(
            self._conn, table_name=table_name, schema=schema_name
        ):
            return None, []

        table = DataTable(
            name=table_name,
            type=None,
            owner=None,
            table_created_at=None,
            table_updated_by=None,
            table_updated_at=None,
            data_size_bytes=None,
            location=None,
            partitions=None,
            raw_description="",
        )

        raw_columns = self._inspect.get_columns(
            table_name=table_name, schema=schema_name
        )
        columns = list(
            map(
                lambda col: DataColumn(
                    name=col["name"],
                    type=str(col["type"]),
                    comment=f"Default:{col['default']} Nullable:{col['nullable']}",
                ),
                raw_columns,
            )
        )

        return table, columns

    @property
    def _is_postgresql(self) -> bool:
        return self._engine.dialect.name == "postgresql"

    def get_all_functions_in_schema(
        self, schema_name: str
    ) -> List[Dict]:
        """Get all functions and procedures in a schema.

        Returns a list of dicts with keys:
            routine_name, routine_type, identity_args, return_type,
            routine_language, routine_definition, owner
        """
        result = self._conn.execute(
            _PG_FUNCTIONS_QUERY, {"schema_name": schema_name}
        )
        return [dict(row) for row in result]

    def get_function_and_params(
        self, schema_name: str, func_info: Dict
    ) -> Tuple[DataTable, List[DataColumn]]:
        """Convert a function/procedure row into DataTable + DataColumn params."""
        routine_name = func_info["routine_name"]
        routine_type = func_info["routine_type"]
        identity_args = func_info.get("identity_args", "")

        # Build a unique display name: name(arg_types) to distinguish overloads
        display_name = routine_name
        if identity_args:
            display_name = f"{routine_name}({identity_args})"

        custom_properties = {
            "language": func_info.get("routine_language", ""),
            "return_type": func_info.get("return_type", ""),
        }

        table = DataTable(
            name=display_name,
            type=routine_type,
            owner=func_info.get("owner"),
            table_created_at=None,
            table_updated_by=None,
            table_updated_at=None,
            data_size_bytes=None,
            location=None,
            partitions=None,
            raw_description=func_info.get("routine_definition", ""),
            custom_properties=custom_properties,
        )

        # Get parameters via information_schema
        # specific_name in information_schema is proname_oid format
        # We parse params from identity_args instead for reliability
        columns = self._parse_identity_args(identity_args)

        return table, columns

    def _parse_identity_args(self, identity_args: str) -> List[DataColumn]:
        """Parse pg_get_function_identity_arguments output into DataColumns.

        Format examples:
            'x integer, y text'
            'IN x integer, OUT result text'
            'INOUT val integer'
            '' (no params)
        """
        if not identity_args or not identity_args.strip():
            return []

        columns = []
        for arg in identity_args.split(","):
            arg = arg.strip()
            if not arg:
                continue

            parts = arg.split()
            mode = "IN"
            if parts[0].upper() in ("IN", "OUT", "INOUT", "VARIADIC"):
                mode = parts[0].upper()
                parts = parts[1:]

            if len(parts) >= 2:
                param_name = parts[0]
                param_type = " ".join(parts[1:])
            elif len(parts) == 1:
                # unnamed parameter, only type
                param_name = ""
                param_type = parts[0]
            else:
                continue

            columns.append(
                DataColumn(
                    name=param_name or f"${len(columns) + 1}",
                    type=param_type,
                    comment=mode,
                )
            )

        return columns

    def load(self):
        super().load()

        if self._is_postgresql:
            self._load_functions()

    def _load_functions(self):
        """Load all functions and procedures from PostgreSQL schemas."""
        from app.db import DBSession
        from lib.metastore.base_metastore_loader import delete_table_not_in_metastore
        from logic.metastore import (
            create_schema,
            get_schema_by_name,
        )

        schema_names = self._get_all_filtered_schema_names()

        for schema_name in schema_names:
            try:
                func_infos = self.get_all_functions_in_schema(schema_name)
            except Exception:
                LOG.error(
                    f"Failed to load functions for schema {schema_name}",
                    exc_info=True,
                )
                continue

            if not func_infos:
                continue

            func_names = set()
            schema_functions = []

            for func_info in func_infos:
                table, columns = self.get_function_and_params(
                    schema_name, func_info
                )
                func_names.add(table.name)
                schema_functions.append((table, columns))

            with DBSession() as session:
                db_schema = get_schema_by_name(
                    schema_name, self.metastore_id, session=session
                )
                if db_schema is None:
                    db_schema = create_schema(
                        name=schema_name,
                        table_count=0,
                        metastore_id=self.metastore_id,
                        session=session,
                    )

                for table, columns in schema_functions:
                    self._create_table_table(
                        db_schema.id,
                        schema_name,
                        table.name,
                        table=table,
                        columns=columns,
                        from_batch=True,
                        session=session,
                    )

    def _get_sqlalchemy(self, metastore_dict):
        from sqlalchemy.engine import reflection

        engine = create_sqlalchemy_engine(metastore_dict["metastore_params"])
        inspect = reflection.Inspector.from_engine(engine)
        conn = engine.connect()

        return engine, inspect, conn
