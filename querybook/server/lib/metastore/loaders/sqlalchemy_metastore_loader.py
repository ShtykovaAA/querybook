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

_PG_SEQUENCES_QUERY = text("""
    SELECT sequencename, start_value, min_value, max_value,
           increment_by, cycle, last_value, data_type
    FROM pg_sequences
    WHERE schemaname = :schema_name
""")

_PG_INDEXES_QUERY = text("""
    SELECT
        i.relname AS index_name,
        t.relname AS table_name,
        ix.indisunique AS is_unique,
        ix.indisprimary AS is_primary,
        am.amname AS index_type,
        pg_get_indexdef(ix.indexrelid) AS index_definition,
        array_agg(a.attname ORDER BY array_position(ix.indkey, a.attnum)) AS columns
    FROM pg_catalog.pg_index ix
    JOIN pg_catalog.pg_class i ON i.oid = ix.indexrelid
    JOIN pg_catalog.pg_class t ON t.oid = ix.indrelid
    JOIN pg_catalog.pg_namespace n ON n.oid = i.relnamespace
    JOIN pg_catalog.pg_am am ON am.oid = i.relam
    LEFT JOIN pg_catalog.pg_attribute a
        ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
    WHERE n.nspname = :schema_name
    GROUP BY i.relname, t.relname, ix.indisunique, ix.indisprimary,
             am.amname, ix.indexrelid
""")

_PG_FOREIGN_TABLE_INFO_QUERY = text("""
    SELECT s.srvname AS server_name,
           ft.ftoptions AS table_options
    FROM pg_catalog.pg_foreign_table ft
    JOIN pg_catalog.pg_class c ON c.oid = ft.ftrelid
    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    JOIN pg_catalog.pg_foreign_server s ON s.oid = ft.ftserver
    WHERE n.nspname = :schema_name AND c.relname = :table_name
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
        self._foreign_table_names: set = set()
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

        tables = list(self._inspect.get_table_names(schema=schema_name))

        if self._is_postgresql:
            try:
                foreign_tables = self._inspect.get_foreign_table_names(
                    schema=schema_name
                )
                self._foreign_table_names.update(
                    f"{schema_name}.{ft}" for ft in foreign_tables
                )
                tables.extend(foreign_tables)
            except Exception:
                pass

        return tables

    def get_table_and_columns(
        self, schema_name, table_name
    ) -> Tuple[DataTable, List[DataColumn]]:
        if not self._engine.dialect.has_table(
            self._conn, table_name=table_name, schema=schema_name
        ):
            return None, []

        raw_columns = self._inspect.get_columns(
            table_name=table_name, schema=schema_name
        )

        raw_description = ""
        if self._is_postgresql:
            try:
                raw_description = self._generate_table_ddl(
                    schema_name, table_name, raw_columns
                )
            except Exception:
                LOG.error(
                    f"Failed to generate DDL for {schema_name}.{table_name}",
                    exc_info=True,
                )

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
            raw_description=raw_description,
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
            self._load_views()
            self._load_materialized_views()
            self._load_sequences()
            self._load_indexes()

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

            function_names = set()
            procedure_names = set()
            schema_functions = []

            for func_info in func_infos:
                table, columns = self.get_function_and_params(
                    schema_name, func_info
                )
                if table.type == "function":
                    function_names.add(table.name)
                else:
                    procedure_names.add(table.name)
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

                delete_table_not_in_metastore(
                    db_schema.id, function_names, table_type="function", session=session
                )
                delete_table_not_in_metastore(
                    db_schema.id, procedure_names, table_type="procedure", session=session
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

    def _load_views(self):
        """Load all plain views from PostgreSQL schemas."""
        self._load_views_by_kind("plain", "view")

    def _load_materialized_views(self):
        """Load all materialized views from PostgreSQL schemas."""
        self._load_views_by_kind("materialized", "materialized_view")

    def _load_views_by_kind(self, include_kind: str, table_type: str):
        """Load views of a given kind (plain or materialized) from all schemas."""
        from app.db import DBSession
        from lib.metastore.base_metastore_loader import delete_table_not_in_metastore
        from logic.metastore import (
            create_schema,
            get_schema_by_name,
        )

        schema_names = self._get_all_filtered_schema_names()

        for schema_name in schema_names:
            try:
                view_names = self._inspect.get_view_names(
                    schema=schema_name, include=(include_kind,)
                )
            except Exception:
                LOG.error(
                    f"Failed to load {table_type}s for schema {schema_name}",
                    exc_info=True,
                )
                continue

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

                delete_table_not_in_metastore(
                    db_schema.id,
                    set(view_names),
                    table_type=table_type,
                    session=session,
                )

                for view_name in view_names:
                    table, columns = self._get_view_and_columns(
                        schema_name, view_name, table_type
                    )
                    self._create_table_table(
                        db_schema.id,
                        schema_name,
                        view_name,
                        table=table,
                        columns=columns,
                        from_batch=True,
                        session=session,
                    )

    def _get_view_and_columns(
        self, schema_name: str, view_name: str, table_type: str
    ) -> Tuple[DataTable, List[DataColumn]]:
        """Get view/materialized view metadata and columns."""
        try:
            definition = self._inspect.get_view_definition(
                view_name, schema=schema_name
            )
        except Exception:
            definition = ""

        custom_properties = None
        if table_type == "materialized_view":
            try:
                indexes = self._inspect.get_indexes(
                    view_name, schema=schema_name
                )
                if indexes:
                    idx_strs = [
                        "{name} ({cols}){unique}".format(
                            name=idx["name"],
                            cols=", ".join(idx.get("column_names", [])),
                            unique=" UNIQUE" if idx.get("unique") else "",
                        )
                        for idx in indexes
                    ]
                    custom_properties = {"indexes": ", ".join(idx_strs)}
            except Exception:
                pass

        table = DataTable(
            name=view_name,
            type=table_type,
            raw_description=definition or "",
            custom_properties=custom_properties,
        )

        try:
            raw_columns = self._inspect.get_columns(
                view_name, schema=schema_name
            )
            columns = [
                DataColumn(
                    name=col["name"],
                    type=str(col["type"]),
                    comment=f"Default:{col['default']} Nullable:{col['nullable']}",
                )
                for col in raw_columns
            ]
        except Exception:
            LOG.error(
                f"Failed to get columns for {table_type} {schema_name}.{view_name}",
                exc_info=True,
            )
            columns = []

        return table, columns

    def _load_sequences(self):
        """Load all sequences from PostgreSQL schemas."""
        from app.db import DBSession
        from lib.metastore.base_metastore_loader import delete_table_not_in_metastore
        from logic.metastore import (
            create_schema,
            get_schema_by_name,
        )

        # Fetch all sequence properties in one query per schema
        schema_names = self._get_all_filtered_schema_names()

        for schema_name in schema_names:
            try:
                seq_names = self._inspect.get_sequence_names(schema=schema_name)
            except Exception:
                LOG.error(
                    f"Failed to load sequences for schema {schema_name}",
                    exc_info=True,
                )
                continue

            # Get properties for all sequences in this schema
            seq_props = {}
            try:
                result = self._conn.execute(
                    _PG_SEQUENCES_QUERY, {"schema_name": schema_name}
                )
                for row in result:
                    row_dict = dict(row)
                    seq_props[row_dict["sequencename"]] = row_dict
            except Exception:
                LOG.error(
                    f"Failed to load sequence properties for schema {schema_name}",
                    exc_info=True,
                )

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

                delete_table_not_in_metastore(
                    db_schema.id,
                    set(seq_names),
                    table_type="sequence",
                    session=session,
                )

                for seq_name in seq_names:
                    props = seq_props.get(seq_name, {})
                    custom_properties = {
                        k: str(v)
                        for k, v in {
                            "start_value": props.get("start_value"),
                            "min_value": props.get("min_value"),
                            "max_value": props.get("max_value"),
                            "increment_by": props.get("increment_by"),
                            "data_type": props.get("data_type"),
                            "cycle": props.get("cycle"),
                            "last_value": props.get("last_value"),
                        }.items()
                        if v is not None
                    }

                    raw_description = ""
                    if props:
                        try:
                            raw_description = self._generate_sequence_ddl(
                                seq_name, schema_name, props
                            )
                        except Exception:
                            pass

                    table = DataTable(
                        name=seq_name,
                        type="sequence",
                        raw_description=raw_description,
                        custom_properties=custom_properties or None,
                    )

                    self._create_table_table(
                        db_schema.id,
                        schema_name,
                        seq_name,
                        table=table,
                        columns=[],
                        from_batch=True,
                        session=session,
                    )

    def _load_indexes(self):
        """Load all indexes from PostgreSQL schemas."""
        from app.db import DBSession
        from lib.metastore.base_metastore_loader import delete_table_not_in_metastore
        from logic.metastore import (
            create_schema,
            get_schema_by_name,
        )

        schema_names = self._get_all_filtered_schema_names()

        for schema_name in schema_names:
            try:
                result = self._conn.execute(
                    _PG_INDEXES_QUERY, {"schema_name": schema_name}
                )
                index_rows = [dict(row) for row in result]
            except Exception:
                LOG.error(
                    f"Failed to load indexes for schema {schema_name}",
                    exc_info=True,
                )
                continue

            index_names = set()
            index_objects = []

            for row in index_rows:
                idx_name = row["index_name"]
                index_names.add(idx_name)

                col_names = [c for c in (row.get("columns") or []) if c]
                columns = [
                    DataColumn(
                        name=col_name,
                        type=row.get("index_type", ""),
                    )
                    for col_name in col_names
                ]

                custom_properties = {
                    "table": row.get("table_name", ""),
                    "index_type": row.get("index_type", ""),
                    "is_unique": str(row.get("is_unique", False)),
                    "is_primary": str(row.get("is_primary", False)),
                }

                table = DataTable(
                    name=idx_name,
                    type="index",
                    raw_description=row.get("index_definition", ""),
                    custom_properties=custom_properties,
                )
                index_objects.append((table, columns))

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

                delete_table_not_in_metastore(
                    db_schema.id,
                    index_names,
                    table_type="index",
                    session=session,
                )

                for table, columns in index_objects:
                    self._create_table_table(
                        db_schema.id,
                        schema_name,
                        table.name,
                        table=table,
                        columns=columns,
                        from_batch=True,
                        session=session,
                    )

    def _generate_table_ddl(
        self, schema_name: str, table_name: str, raw_columns: list
    ) -> str:
        """Generate CREATE TABLE DDL from inspector metadata."""
        is_foreign = f"{schema_name}.{table_name}" in self._foreign_table_names
        parts = []

        # Column definitions
        col_defs = []
        for col in raw_columns:
            col_def = f'    "{col["name"]}" {col["type"]}'
            if col.get("default") is not None:
                col_def += f" DEFAULT {col['default']}"
            if not col.get("nullable", True):
                col_def += " NOT NULL"
            col_defs.append(col_def)

        # Constraints (not applicable for foreign tables)
        constraint_defs = []
        if not is_foreign:
            pk = self._inspect.get_pk_constraint(table_name, schema=schema_name)
            if pk and pk.get("constrained_columns"):
                cols = ", ".join(f'"{c}"' for c in pk["constrained_columns"])
                name = f'CONSTRAINT "{pk["name"]}" ' if pk.get("name") else ""
                constraint_defs.append(f"    {name}PRIMARY KEY ({cols})")

            try:
                for uq in self._inspect.get_unique_constraints(
                    table_name, schema=schema_name
                ):
                    cols = ", ".join(f'"{c}"' for c in uq["column_names"])
                    name = f'CONSTRAINT "{uq["name"]}" ' if uq.get("name") else ""
                    constraint_defs.append(f"    {name}UNIQUE ({cols})")
            except Exception:
                pass

            try:
                for ck in self._inspect.get_check_constraints(
                    table_name, schema=schema_name
                ):
                    name = f'CONSTRAINT "{ck["name"]}" ' if ck.get("name") else ""
                    constraint_defs.append(f"    {name}CHECK ({ck['sqltext']})")
            except Exception:
                pass

            for fk in self._inspect.get_foreign_keys(
                table_name, schema=schema_name
            ):
                cols = ", ".join(f'"{c}"' for c in fk["constrained_columns"])
                ref_schema = fk.get("referred_schema") or "public"
                ref_table = fk["referred_table"]
                ref_cols = ", ".join(f'"{c}"' for c in fk["referred_columns"])
                name = f'CONSTRAINT "{fk["name"]}" ' if fk.get("name") else ""
                constraint_defs.append(
                    f'    {name}FOREIGN KEY ({cols}) '
                    f'REFERENCES "{ref_schema}"."{ref_table}" ({ref_cols})'
                )

        # Assemble CREATE TABLE
        all_defs = col_defs + constraint_defs
        body = ",\n".join(all_defs)

        if is_foreign:
            header = f'CREATE FOREIGN TABLE "{schema_name}"."{table_name}"'
        else:
            header = f'CREATE TABLE "{schema_name}"."{table_name}"'

        parts.append(f"{header} (\n{body}\n)")

        # Foreign table: add SERVER clause
        if is_foreign:
            try:
                result = self._conn.execute(
                    _PG_FOREIGN_TABLE_INFO_QUERY,
                    {"schema_name": schema_name, "table_name": table_name},
                )
                row = result.fetchone()
                if row:
                    row_dict = dict(row)
                    server_name = row_dict.get("server_name", "")
                    parts[-1] += f"\nSERVER {server_name}"
                    options = row_dict.get("table_options")
                    if options:
                        opts_str = ", ".join(options)
                        parts[-1] += f"\nOPTIONS ({opts_str})"
            except Exception:
                pass

        parts[-1] += ";"

        # Indexes (not for foreign tables)
        if not is_foreign:
            try:
                for idx in self._inspect.get_indexes(
                    table_name, schema=schema_name
                ):
                    unique = "UNIQUE " if idx.get("unique") else ""
                    idx_cols = ", ".join(
                        f'"{c}"' for c in idx.get("column_names", []) if c
                    )
                    if idx_cols:
                        parts.append(
                            f'CREATE {unique}INDEX "{idx["name"]}" '
                            f'ON "{schema_name}"."{table_name}" ({idx_cols});'
                        )
            except Exception:
                pass

        # Comment
        try:
            comment = self._inspect.get_table_comment(
                table_name, schema=schema_name
            )
            if comment and comment.get("text"):
                escaped = comment["text"].replace("'", "''")
                parts.append(
                    f"COMMENT ON TABLE \"{schema_name}\".\"{table_name}\" "
                    f"IS '{escaped}';"
                )
        except Exception:
            pass

        return "\n\n".join(parts)

    def _generate_sequence_ddl(
        self, seq_name: str, schema_name: str, props: dict
    ) -> str:
        """Generate CREATE SEQUENCE DDL from sequence properties."""
        lines = [f'CREATE SEQUENCE "{schema_name}"."{seq_name}"']
        if props.get("data_type"):
            lines.append(f"    AS {props['data_type']}")
        if props.get("start_value") is not None:
            lines.append(f"    START WITH {props['start_value']}")
        if props.get("increment_by") is not None:
            lines.append(f"    INCREMENT BY {props['increment_by']}")
        if props.get("min_value") is not None:
            lines.append(f"    MINVALUE {props['min_value']}")
        if props.get("max_value") is not None:
            lines.append(f"    MAXVALUE {props['max_value']}")
        if props.get("cycle"):
            lines.append("    CYCLE")
        else:
            lines.append("    NO CYCLE")
        return "\n".join(lines) + ";"

    def _get_sqlalchemy(self, metastore_dict):
        from sqlalchemy.engine import reflection

        engine = create_sqlalchemy_engine(metastore_dict["metastore_params"])
        inspect = reflection.Inspector.from_engine(engine)
        conn = engine.connect()

        return engine, inspect, conn
