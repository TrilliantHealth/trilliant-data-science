from typing import Optional, Tuple

from .. import metaschema
from .util import AUTOGEN_DISCLAIMER

SQL_TABLE_SCHEMA_TEMPLATE = """CREATE TABLE {name}(
    {columns}
);"""


def index_name(table_name: str, *index_columns: str) -> str:
    return f"idx_{table_name}_{'_'.join(index_columns)}".lower()


def render_sql_table_schema(table: metaschema.Table) -> str:
    column_defs = []
    for column in table.columns:
        column_def = f"{column.snake_case_name} {column.dtype.sqlite}"
        if not column.nullable:
            column_def = f"{column_def} NOT NULL"
        column_defs.append(column_def)

    return SQL_TABLE_SCHEMA_TEMPLATE.format(
        name=table.snake_case_name, columns=",\n    ".join(column_defs)
    )


def render_sql_index_schema(table: metaschema.Table) -> Optional[str]:
    unique_constraints = {frozenset(c.unique) for c in table.unique_constraints}
    index_defs = []
    if table.primary_key:
        table_constraints = (
            f"CREATE UNIQUE INDEX {index_name(table.snake_case_name, *table.primary_key)} ON "
            f'{table.snake_case_name}({", ".join(table.primary_key)});'
        )
        index_defs.append(table_constraints)

    for index in table.indexes:
        unique = "UNIQUE " if frozenset(index) in unique_constraints else ""
        index_def = (
            f"CREATE {unique}INDEX {index_name(table.snake_case_name, *index)} "
            f'ON {table.snake_case_name}({", ".join(index)});'
        )
        index_defs.append(index_def)

    return "\n\n".join(index_defs) if len(index_defs) else None


def render_sql_schema(schema: metaschema.Schema) -> Tuple[str, str]:
    """Render SQL Create Table and Index DDL

    :param schema: input metaschema definition to generate SQL DDL from

    :return: Returns a two tuple where the first item is the create table DDL and the second is the
      create index DDL
    :rtype: Tuple[str, str]
    """
    defs = []
    index_defs = []
    for table in schema.package_tables:
        defs.append(render_sql_table_schema(table))
        index_defs.append(render_sql_index_schema(table))

    create_table_ddl = f"-- {AUTOGEN_DISCLAIMER}\n\n" + "\n\n".join(defs).strip() + "\n"
    create_index_ddl = (
        f"-- {AUTOGEN_DISCLAIMER}\n\n" + "\n\n".join(filter(None, index_defs)).strip() + "\n"
    )

    return create_table_ddl, create_index_ddl
