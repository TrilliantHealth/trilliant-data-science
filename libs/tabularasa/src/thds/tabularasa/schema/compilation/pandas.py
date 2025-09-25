import itertools
from typing import List, NamedTuple, Optional

import numpy as np
import pandas.core.dtypes.dtypes as pd_dtypes
import pandera as pa

import thds.tabularasa.loaders.util
from thds.tabularasa.schema import metaschema

from ._format import autoformat
from .util import (
    AUTOGEN_DISCLAIMER,
    VarName,
    _dict_literal,
    _indent,
    _list_literal,
    constructor_template,
    render_blob_store_def,
    render_constructor,
)

REMOTE_BLOB_STORE_VAR_NAME = "REMOTE_BLOB_STORE"

PANDERA_DATAFRAME_SCHEMA_TEMPLATE = (
    """pa.%s(
    columns={columns},
    index={index},
    checks={checks},
    coerce={coerce!r},
    strict={strict!r},
    ordered={ordered!r},
)"""
    % pa.DataFrameSchema.__name__
)

PANDERA_COLUMN_SCHEMA_TEMPLATE = (
    """pa.%s(
    {dtype},
    checks={checks},
    nullable={nullable!r},
    unique={unique!r},
)"""
    % pa.Column.__name__
)

PANDERA_INDEX_SCHEMA_TEMPLATE = (
    """pa.%s(
    {dtype},
    checks={checks},
    nullable={nullable!r},
    unique={unique!r},
    name={name!r},
)"""
    % pa.Index.__name__
)

PANDERA_MULTIINDEX_SCHEMA_TEMPLATE = (
    """pa.%s(
    [
        {indexes},
    ],
    strict={strict!r},
    ordered=True,
)"""
    % pa.MultiIndex.__name__
)

PANDAS_LOADER_TEMPLATE = constructor_template(
    thds.tabularasa.loaders.util.PandasParquetLoader.from_pandera_schema,
    module_name=(
        f"{thds.tabularasa.loaders.util.__name__}.{thds.tabularasa.loaders.util.PandasParquetLoader.__name__}"
    ),
    exclude=["filename"],
)


def render_pandera_table_schema(table: metaschema.Table, coerce_run_time_tables: bool) -> str:
    proxy_schema: metaschema._DataFrameSchemaProxy
    proxy_schema = metaschema.render_pandera_schema(table, as_str=True)  # type: ignore

    def render_check_exprs(check_exprs: Optional[List[str]]) -> str:
        if check_exprs:
            return _indent(_list_literal(check_exprs, linebreak=False), 1)
        return repr(None)

    def render_column_schema(schema: metaschema._ColumnSchemaProxy) -> str:
        return PANDERA_COLUMN_SCHEMA_TEMPLATE.format(
            dtype=schema.dtype,
            checks=render_check_exprs(schema.checks),
            nullable=schema.nullable,
            unique=schema.unique,
        )

    def render_index_schema(schema: metaschema._IndexSchemaProxy) -> str:
        return PANDERA_INDEX_SCHEMA_TEMPLATE.format(
            dtype=schema.dtype,
            checks=render_check_exprs(schema.checks),
            nullable=schema.nullable,
            unique=schema.unique,
            name=schema.name,
        )

    def render_multiindex_schema(schema: metaschema._MultiIndexSchemaProxy) -> str:
        indexes = ",\n".join(render_index_schema(index) for index in schema.indexes)
        return PANDERA_MULTIINDEX_SCHEMA_TEMPLATE.format(
            indexes=_indent(indexes, 2),
            strict=schema.strict,
        )

    column_defs = [
        (name, _indent(render_column_schema(column))) for name, column in proxy_schema.columns.items()
    ]
    column_def = _indent(_dict_literal(column_defs), 1) if column_defs else "{}"
    if isinstance(proxy_schema.index, metaschema._IndexSchemaProxy):
        index_def = _indent(render_index_schema(proxy_schema.index), 1)
    elif isinstance(proxy_schema.index, metaschema._MultiIndexSchemaProxy):
        index_def = _indent(render_multiindex_schema(proxy_schema.index), 1)
    else:
        index_def = repr(None)

    if proxy_schema.checks:
        checks = _indent(_list_literal(proxy_schema.checks, linebreak=False))
    else:
        checks = repr(None)

    table_schema = PANDERA_DATAFRAME_SCHEMA_TEMPLATE.format(
        columns=column_def,
        index=index_def,
        checks=checks,
        strict=True,
        # allow the pandera schema to coerce inputs if the table is dropped in at run time, in case e.g.
        # we expected int32 but got int64, which is a non-fatal error
        coerce=table.run_time_installed if coerce_run_time_tables else False,
        ordered=False,
    )
    return f"{table.snake_case_name}_schema = {table_schema}"


class ImportsAndCode(NamedTuple):
    imports: List[str]
    code: List[str]


def render_pandera_loaders(
    schema: metaschema.Schema,
    package: str,
) -> ImportsAndCode:
    data_dir = schema.build_options.package_data_dir
    render_pyarrow_schemas = schema.build_options.pyarrow
    qualified_pyarrow_module_name = "pyarrow_schemas"
    import_lines = list()
    if render_pyarrow_schemas:
        import_lines.append("\n")
        import_lines.append(f"from . import pyarrow as {qualified_pyarrow_module_name}")
    return ImportsAndCode(
        import_lines,
        [
            render_constructor(
                PANDAS_LOADER_TEMPLATE,
                kwargs=dict(
                    table_name=table.snake_case_name,
                    schema=VarName(f"{table.snake_case_name}_schema"),
                    package=package,
                    data_dir=data_dir,
                    blob_store=(
                        None if schema.remote_blob_store is None else VarName(REMOTE_BLOB_STORE_VAR_NAME)
                    ),
                    md5=table.md5,
                    pyarrow_schema=(
                        VarName(
                            f"{qualified_pyarrow_module_name}.{table.snake_case_name}_pyarrow_schema"
                        )
                        if render_pyarrow_schemas
                        else None
                    ),
                ),
                var_name=f"load_{table.snake_case_name}",
            )
            for table in schema.package_tables
        ],
    )


def render_pandera_module(
    schema: metaschema.Schema,
    package: str,
    coerce_run_time_tables: bool = False,
    loader_defs: Optional[ImportsAndCode] = None,
) -> str:
    if loader_defs is None:
        loader_defs = (
            render_pandera_loaders(
                schema,
                package=package,
            )
            if schema.build_options.package_data_dir
            else None
        )

    # stdlib imports
    all_constraints = itertools.chain.from_iterable(t.constraints for t in schema.types.values())
    required_stdlib_modules = sorted(
        set(itertools.chain.from_iterable(c.required_modules() for c in all_constraints))
    )
    all_dtypes = set(
        itertools.chain.from_iterable(
            (c.pandas(index=c.name in (t.primary_key or [])) for c in t.columns)
            for t in schema.package_tables
        )
    )

    table_schemas = "\n\n".join(
        render_pandera_table_schema(table, coerce_run_time_tables=coerce_run_time_tables)
        for table in schema.package_tables
    )

    any_np_dtypes = any(isinstance(dt, np.dtype) for dt in all_dtypes)

    import_lines = ["import " + modname + "\n" for modname in required_stdlib_modules]
    if import_lines:
        import_lines.append("\n")
    if any_np_dtypes:
        import_lines.append("import numpy as np\n")
    if any(isinstance(dt, pd_dtypes.ExtensionDtype) for dt in all_dtypes):
        import_lines.append("import pandas as pd\n")
    import_lines.append("import pandera as pa\n")
    import_lines.append("\n")
    if any_np_dtypes:
        import_lines.append(f"import {thds.tabularasa.compat.__name__}  # noqa: F401\n")
        # is there an effective way to check if we have any np numeric dtypes as indices so I can leave out the 'noqa'?
    if loader_defs:
        import_lines.append(f"import {thds.tabularasa.loaders.util.__name__}\n")
    if schema.remote_blob_store is not None:
        import_lines.append(f"import {thds.tabularasa.schema.files.__name__}\n")

    if loader_defs:
        import_lines += loader_defs.imports

    imports = "".join(import_lines)

    global_var_defs = []
    if schema.remote_blob_store is not None:
        global_var_defs.append(
            render_blob_store_def(schema.remote_blob_store, REMOTE_BLOB_STORE_VAR_NAME)
        )
    globals_ = "\n".join(global_var_defs)
    loaders = "\n\n".join(loader_defs.code) if loader_defs else ""

    return autoformat(
        f"{imports}\n# {AUTOGEN_DISCLAIMER}\n\n{globals_}\n\n{table_schemas}\n\n{loaders}\n"
    )
