from itertools import chain
from logging import getLogger
from typing import Dict, List, Optional, Tuple

import thds.tabularasa.loaders.util
import thds.tabularasa.schema
from thds.tabularasa.schema import metaschema
from thds.tabularasa.schema.compilation.attrs import render_attr_field_def

from ._format import autoformat
from .sqlite import index_name
from .util import AUTOGEN_DISCLAIMER, sorted_class_names_for_import

_LOGGER = getLogger(__name__)

PACKAGE_VARNAME = "PACKAGE"
DB_PATH_VARNAME = "DB_PATH"

ATTRS_MODULE_NAME = ".attrs"

LINE_WIDTH = 88

COLUMN_LINESEP = ",\n                  "

ATTRS_CLASS_LOADER_TEMPLATE = """class {class_name}Loader:

    def __init__(self, db: util.%s):
        self._db = db
        self._record = util.%s({class_name})

    {accessors}
""" % (
    thds.tabularasa.loaders.util.AttrsSQLiteDatabase.__name__,
    thds.tabularasa.loaders.sqlite_util.sqlite_constructor_for_record_type.__name__,  # type: ignore
)

ATTRS_INDEX_ACCESSOR_TEMPLATE = """
    def {method_name}(self, {typed_args}) -> typing.{return_type}[{class_name}]:
        return self._db.sqlite_{index_kind}_query(
            self._record,
            \"\"\"
            SELECT
              {columns}
            FROM {table_name}
            INDEXED BY {index_name}
            WHERE {condition};
            \"\"\",
            ({args},),
        )
"""

ATTRS_BULK_INDEX_ACCESSOR_TEMPLATE = """
    def {method_name}_bulk(self, {arg_name}: typing.Collection[{typed_args}]) -> typing.{return_type}[{class_name}]:
        if {arg_name}:
            return self._db.sqlite_{index_kind}_query(
                self._record,
                f\"\"\"
                SELECT
                  {columns}
                FROM {table_name}
                INDEXED BY {index_name}
                WHERE {condition};
                \"\"\",
                {arg_name},
                single_col={single_col},
            )
        else:
            return iter(())
"""

ATTRS_MAIN_LOADER_TEMPLATE = """class SQLiteLoader:
    def __init__(
        self,
        package: typing.Optional[str] = %s,
        db_path: str = %s,
        cache_size: int = util.DEFAULT_ATTR_SQLITE_CACHE_SIZE,
        mmap_size: int = util.DEFAULT_MMAP_BYTES,
    ):
        self._db = util.%s(package=package, db_path=db_path, cache_size=cache_size, mmap_size=mmap_size)
        {table_loaders}
""" % (
    PACKAGE_VARNAME,
    DB_PATH_VARNAME,
    thds.tabularasa.loaders.util.AttrsSQLiteDatabase.__name__,
)


def render_attrs_loader_schema(table: metaschema.Table, build_options: metaschema.BuildOptions) -> str:
    accessor_defs = []
    column_lookup = {col.name: col for col in table.columns}
    unq_constraints = {frozenset(c.unique) for c in table.unique_constraints}

    if table.primary_key:
        accessor_defs.append(
            render_accessor_method(
                table, table.primary_key, column_lookup, pk=True, build_options=build_options
            )
        )
        accessor_defs.append(
            render_accessor_method(
                table, table.primary_key, column_lookup, pk=True, bulk=True, build_options=build_options
            )
        )

    for idx in table.indexes:
        unique = frozenset(idx) in unq_constraints
        accessor_defs.append(
            render_accessor_method(
                table, idx, column_lookup, pk=False, unique=unique, build_options=build_options
            )
        )
        accessor_defs.append(
            render_accessor_method(
                table,
                idx,
                column_lookup,
                pk=False,
                unique=unique,
                bulk=True,
                build_options=build_options,
            )
        )

    accessors = "".join(accessor_defs).strip()
    return ATTRS_CLASS_LOADER_TEMPLATE.format(
        class_name=table.class_name,
        accessors=accessors,
    )


def render_accessor_method(
    table: metaschema.Table,
    index_columns: Tuple[str, ...],
    column_lookup: Dict[str, metaschema.Column],
    build_options: metaschema.BuildOptions,
    pk: bool = False,
    unique: bool = False,
    bulk: bool = False,
) -> str:
    index_column_names = tuple(map(metaschema.snake_case, index_columns))
    method_name = "pk" if pk else f"idx_{'_'.join(index_column_names)}"
    index_kind = "bulk" if bulk else ("pk" if pk or unique else "index")
    return_type = "Iterator" if bulk else ("Optional" if pk or unique else "List")
    # we use the `IS` operator to allow for comparison in case of nullable index columns
    arg_name = "__".join(index_column_names)
    nullsafe_condition = " AND ".join([f"{column} IS (?)" for column in index_column_names])
    if bulk:
        columns = [column_lookup[col] for col in index_columns]
        has_null_cols = any(col.nullable for col in columns)
        if len(index_column_names) == 1:
            # for single-column indexes, we need to unpack the single value from the tuple
            typed_args = columns[0].python_type_literal(build_options=build_options, builtin=True)
            if has_null_cols:
                # sqlite IN operator unfortunately doesn't support a NULL == NULL variant, the way that IS does for =
                condition = f"{{' OR '.join(['{index_column_names[0]} IS (?)'] * len({arg_name}))}}"
            else:
                condition = f"{index_column_names[0]} IN ({{','.join('?' * len({arg_name}))}})"
            single_col = "True"
        else:
            type_strs = ", ".join(
                col.python_type_literal(build_options=build_options, builtin=True) for col in columns
            )
            typed_args = f"typing.Tuple[{type_strs}]"
            if has_null_cols:
                # sqlite IN operator unfortunately doesn't support a NULL == NULL variant, the way that IS does for =
                param_tuple = f"({nullsafe_condition})"
                condition = f"{{' OR '.join(['{param_tuple}'] * len({arg_name}))}}"
            else:
                param_tuple = f"({', '.join('?' * len(index_column_names))})"
                condition = f"({', '.join(index_column_names)}) IN ({{','.join(['{param_tuple}'] * len({arg_name}))}})"
            single_col = "False"
    else:
        condition = nullsafe_condition
        typed_args = ", ".join(
            [
                render_attr_field_def(column_lookup[col], builtin=True, build_options=build_options)
                for col in index_columns
            ]
        )
        single_col = ""

    return (ATTRS_BULK_INDEX_ACCESSOR_TEMPLATE if bulk else ATTRS_INDEX_ACCESSOR_TEMPLATE).format(
        class_name=table.class_name,
        method_name=method_name,
        # use builtin types (as opposed to e.g. Literals and Newtypes) to make the API simpler to use
        typed_args=typed_args,
        arg_name=arg_name,
        single_col=single_col,
        return_type=return_type,
        index_kind=index_kind,
        table_name=table.snake_case_name,
        columns=COLUMN_LINESEP.join(c.snake_case_name for c in table.columns),
        index_name=index_name(table.snake_case_name, *index_column_names),
        condition=condition,
        args=", ".join(index_column_names),
    )


def render_attrs_main_loader(
    tables: List[metaschema.Table],
) -> str:
    loader_instance_defs = [
        f"self.{table.snake_case_name} = {table.class_name}Loader(self._db)" for table in tables
    ]
    table_loaders = "\n        ".join(loader_instance_defs)
    return ATTRS_MAIN_LOADER_TEMPLATE.format(table_loaders=table_loaders)


def _import_lines(
    tables: List[metaschema.Table],
    attrs_module_name: Optional[str],
    build_options: metaschema.BuildOptions,
):
    # need typing always for List and Optional
    stdlib_imports = sorted(
        {
            "typing",
            *chain.from_iterable(t.attrs_sqlite_required_imports(build_options) for t in tables),
        }
    )
    import_lines = [f"import {module}\n" for module in stdlib_imports]
    if import_lines:
        import_lines.append("\n")

    import_lines.append(f"import {thds.tabularasa.loaders.sqlite_util.__name__} as util\n")
    import_lines.append("\n")
    if attrs_module_name is not None:
        import_lines.append(f"from {attrs_module_name} import (\n")
        attrs_module_classnames = {table.class_name for table in tables}
        import_lines.extend(
            f"    {name},\n" for name in sorted_class_names_for_import(attrs_module_classnames)
        )
        import_lines.append(")\n")

    return "".join(import_lines)


def _has_index(table: metaschema.Table) -> bool:
    return (table.primary_key is not None) or len(table.indexes) > 0


def render_attrs_sqlite_schema(
    schema: metaschema.Schema,
    package: str = "",
    db_path: str = "",
    attrs_module_name: Optional[str] = ATTRS_MODULE_NAME,
) -> str:
    has_database_loader = bool(package and db_path)

    # do not generate a SQLite loader if there is no primary key or index defined on the table def
    tables = [table for table in schema.package_tables if table.has_indexes]
    tables_filtered = [table.name for table in schema.package_tables if not table.has_indexes]
    if not tables:
        _LOGGER.info(
            f"Skipping SQLite loader generation for all tables: {tables_filtered}; none has any index "
            f"specified"
        )
        return ""

    if tables_filtered:
        _LOGGER.info(
            f"Skipping SQLite loader generation for the following "
            f"tables because no indices or primary keys are defined: {', '.join(tables_filtered)}"
        )

    imports = _import_lines(tables, attrs_module_name, schema.build_options)
    loader_defs = [render_attrs_loader_schema(table, schema.build_options) for table in tables]
    loaders = "\n\n".join(loader_defs)

    if has_database_loader:
        constants = f'{PACKAGE_VARNAME} = "{package}"\n{DB_PATH_VARNAME} = "{db_path}"'
        loader_def = render_attrs_main_loader(tables)
    else:
        constants = ""
        loader_def = ""
    return autoformat(
        f"{imports}\n# {AUTOGEN_DISCLAIMER}\n\n{constants}\n\n\n{loaders}\n\n{loader_def}\n"
    )
