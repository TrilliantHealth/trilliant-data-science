from itertools import chain
from operator import itemgetter
from textwrap import wrap

import typing_extensions

import thds.tabularasa.loaders.util
from thds.tabularasa.schema import metaschema

from .util import (
    AUTOGEN_DISCLAIMER,
    VarName,
    _indent,
    _wrap_lines_with_prefix,
    autoformat,
    constructor_template,
    render_blob_store_def,
    render_constructor,
    sorted_class_names_for_import,
)

DEFAULT_LINE_WIDTH = 88

REMOTE_BLOB_STORE_VAR_NAME = "REMOTE_BLOB_STORE"

DOCSTRING_PARAM_TEMPLATE = """:param {name}: {doc}"""

CUSTOM_TYPE_DEF_TEMPLATE = """{comment}{name} = {type}"""

ATTRS_CLASS_DEF_TEMPLATE = """@attr.s(auto_attribs=True, frozen=True)
class {class_name}:
    \"\"\"{doc}

    {params}
    \"\"\"

    {fields}
"""

ATTRS_FIELD_DEF_TEMPLATE_BASIC = "{name}: {type}"

ATTRS_LOADER_TEMPLATE = constructor_template(
    thds.tabularasa.loaders.util.AttrsParquetLoader, exclude=["filename"]
)


def render_type_def(
    type_: metaschema.CustomType, use_newtypes: bool = False, type_constraint_comments: bool = True
) -> str:
    type_name = type_.class_name
    type_literal = type_.python_type_def_literal
    if use_newtypes and not type_.enum:
        # newtype wrapper for non-enum constrained types
        type_literal = f'typing.NewType("{type_name}", {type_literal})'

    if type_constraint_comments:
        comment = type_.comment
        if comment:
            lines = wrap(comment, DEFAULT_LINE_WIDTH - 2)
            comment = "\n".join("# " + line for line in lines) + "\n"
        else:
            comment = ""
    else:
        comment = ""
    return CUSTOM_TYPE_DEF_TEMPLATE.format(comment=comment, name=type_name, type=type_literal)


def render_attr_field_def(column: metaschema.Column, builtin: bool = False) -> str:
    type_literal = column.python_type_literal(builtin=builtin)
    return ATTRS_FIELD_DEF_TEMPLATE_BASIC.format(name=column.snake_case_name, type=type_literal)


def render_attrs_table_schema(table: metaschema.Table) -> str:
    field_defs = []
    params = []

    for column in table.columns:
        field_def = render_attr_field_def(column, builtin=False)
        field_defs.append(field_def)
        doc = _wrap_lines_with_prefix(
            column.doc,
            DEFAULT_LINE_WIDTH - 4,
            first_line_prefix_len=len(f":param {column.snake_case_name}: "),
            trailing_line_indent=2,
        )
        params.append(
            DOCSTRING_PARAM_TEMPLATE.format(
                name=column.snake_case_name,
                doc=doc,
            )
        )

    table_doc = _wrap_lines_with_prefix(
        table.doc,
        DEFAULT_LINE_WIDTH - 4,
        first_line_prefix_len=3,  # triple quotes
        trailing_line_indent=0,
    )

    return ATTRS_CLASS_DEF_TEMPLATE.format(
        class_name=table.class_name,
        doc=_indent(table_doc),
        params=_indent("\n".join(params)),
        fields=_indent("\n".join(field_defs)),
    )


def render_attrs_schema(
    schema: metaschema.Schema,
    package: str,
    data_dir: str,
    use_newtypes: bool = False,
    type_constraint_comments: bool = True,
    render_pyarrow_schemas: bool = False,
    import_external_types: bool = True,
) -> str:
    # custom types
    referenced_custom_type_refs = set(schema.packaged_custom_type_refs)
    if not import_external_types:
        referenced_custom_type_refs.update(schema.external_type_refs)

    referenced_custom_types = [schema.types[name] for name in referenced_custom_type_refs]
    type_defs = [
        render_type_def(
            type_, use_newtypes=use_newtypes, type_constraint_comments=type_constraint_comments
        )
        for type_ in sorted(referenced_custom_types, key=lambda type_: type_.name)
    ]

    # attrs record types
    table_defs = [render_attrs_table_schema(table) for table in schema.package_tables]

    # imports
    qualified_pyarrow_module_name = "pyarrow_schemas"
    stdlib_imports = sorted(
        set(chain.from_iterable(t.attrs_required_imports for t in schema.package_tables))
    )
    import_lines = [f"import {module}\n" for module in stdlib_imports]
    if import_lines:
        import_lines.append("\n")
    import_lines.append("import attr\n")
    # need typing_extensions e.g. for Literal if it hasn't been added to the std lib typing module yet
    if not metaschema.NEW_TYPING and any(type_.enum is not None for type_ in referenced_custom_types):
        import_lines.append(f"import {typing_extensions.__name__}\n")

    import_lines.append("\n")
    import_lines.append(f"import {thds.tabularasa.loaders.util.__name__}\n")
    if schema.remote_blob_store is not None:
        import_lines.append(f"import {thds.tabularasa.schema.metaschema.__name__}\n")

    if import_external_types:
        # external type imports
        sep = ",\n    "
        for module_name, class_names in sorted(schema.external_type_imports.items(), key=itemgetter(0)):
            import_lines.append(
                f"from {module_name} import (\n    {sep.join(sorted_class_names_for_import(class_names))},\n)\n"
            )

    if render_pyarrow_schemas:
        import_lines.append("\n")
        import_lines.append(f"from . import pyarrow as {qualified_pyarrow_module_name}")

    global_var_defs = []
    if schema.remote_blob_store is not None:
        global_var_defs.append(
            render_blob_store_def(schema.remote_blob_store, REMOTE_BLOB_STORE_VAR_NAME)
        )

    imports = "".join(import_lines)
    globals_ = "\n".join(global_var_defs)
    types = "\n\n".join(type_defs)
    classes = "\n\n".join(table_defs)

    # loaders
    loader_defs = [
        render_constructor(
            ATTRS_LOADER_TEMPLATE,
            kwargs=dict(
                table_name=table.snake_case_name,
                type_=VarName(table.class_name),
                package=package,
                data_dir=data_dir,
                md5=table.md5,
                blob_store=None
                if schema.remote_blob_store is None
                else VarName(REMOTE_BLOB_STORE_VAR_NAME),
                pyarrow_schema=VarName(
                    f"{qualified_pyarrow_module_name}.{table.snake_case_name}_pyarrow_schema"
                )
                if render_pyarrow_schemas
                else None,
            ),
            var_name=f"load_{table.snake_case_name}",
        )
        for table in schema.package_tables
    ]
    loaders = "\n\n".join(loader_defs)

    # module
    return autoformat(
        f"{imports}\n# {AUTOGEN_DISCLAIMER}\n\n{globals_}\n\n{types}\n\n\n{classes}\n\n{loaders}\n"
    )
