import typing as ty
from itertools import chain
from operator import itemgetter
from textwrap import wrap

import typing_extensions

import thds.tabularasa.loaders.util
from thds.tabularasa.schema import metaschema

from ._format import autoformat
from .util import (
    AUTOGEN_DISCLAIMER,
    VarName,
    _indent,
    _wrap_lines_with_prefix,
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
    thds.tabularasa.loaders.util.AttrsParquetLoader,
    exclude=["filename"],
    type_params=["{record_type}"],
)


def render_type_def(
    type_: metaschema.CustomType,
    use_newtypes: bool = False,
    type_constraint_comments: bool = True,
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


PYARROW_SCHEMAS_QUALIFIED_IMPORT = "pyarrow_schemas"


class ImportsAndCode(ty.NamedTuple):
    """Couples code and its required imports."""

    third_party_imports: ty.List[str]
    tabularasa_imports: ty.List[str]
    code: ty.List[str]


def render_attrs_loaders(
    schema: metaschema.Schema,
    package: str,
    data_dir: ty.Optional[str],
    render_pyarrow_schemas: bool = False,
) -> ImportsAndCode:
    import_lines = list()
    if render_pyarrow_schemas:
        import_lines.append("\n")
        import_lines.append(f"from . import pyarrow as {PYARROW_SCHEMAS_QUALIFIED_IMPORT}")

    return ImportsAndCode(
        list(),
        import_lines,
        [
            render_constructor(
                ATTRS_LOADER_TEMPLATE,
                kwargs=dict(
                    record_type=VarName(table.class_name),
                    table_name=table.snake_case_name,
                    type_=VarName(table.class_name),
                    package=package,
                    data_dir=data_dir,
                    md5=table.md5,
                    blob_store=None
                    if schema.remote_blob_store is None
                    else VarName(REMOTE_BLOB_STORE_VAR_NAME),
                    pyarrow_schema=VarName(
                        f"{PYARROW_SCHEMAS_QUALIFIED_IMPORT}.{table.snake_case_name}_pyarrow_schema"
                    )
                    if render_pyarrow_schemas
                    else None,
                ),
                var_name=f"load_{table.snake_case_name}",
            )
            for table in schema.package_tables
        ],
    )


def render_attrs_type_defs(
    schema: metaschema.Schema,
    use_newtypes: bool,
    type_constraint_comments: bool,
    import_external_types: bool,
) -> ImportsAndCode:
    # custom types
    referenced_custom_type_refs = set(schema.packaged_custom_type_refs)
    if not import_external_types:
        referenced_custom_type_refs.update(schema.external_type_refs)

    referenced_custom_types = [schema.types[name] for name in referenced_custom_type_refs]
    type_defs = [
        render_type_def(
            type_,
            use_newtypes=use_newtypes,
            type_constraint_comments=type_constraint_comments,
        )
        for type_ in sorted(referenced_custom_types, key=lambda type_: type_.name)
    ]

    # need typing_extensions e.g. for Literal if it hasn't been added to the std lib typing module yet
    third_party_import_lines = (
        [f"import {typing_extensions.__name__}\n"]
        if (
            not metaschema.NEW_TYPING
            and any(type_.enum is not None for type_ in referenced_custom_types)
        )
        else list()
    )

    import_lines = list()
    if import_external_types:
        # external type imports
        sep = ",\n    "
        for module_name, class_names in sorted(schema.external_type_imports.items(), key=itemgetter(0)):
            import_lines.append(
                f"from {module_name} import (\n    {sep.join(sorted_class_names_for_import(class_names))},\n)\n"
            )

    return ImportsAndCode(third_party_import_lines, import_lines, type_defs)


def _render_attrs_schema(
    schema: metaschema.Schema,
    type_defs: ImportsAndCode,
    loader_defs: ty.Optional[ImportsAndCode],
) -> str:
    loader_defs = loader_defs or ImportsAndCode([], [], [])
    assert loader_defs, "Loaders are optional but the line above is not"
    # attrs record types
    table_defs = [render_attrs_table_schema(table) for table in schema.package_tables]

    # imports
    stdlib_imports = sorted(
        set(chain.from_iterable(t.attrs_required_imports for t in schema.package_tables))
    )
    import_lines = [f"import {module}\n" for module in stdlib_imports]
    if import_lines:
        import_lines.append("\n")
    import_lines.append("import attr\n")
    import_lines += type_defs.third_party_imports
    import_lines += loader_defs.third_party_imports

    import_lines.append("\n")

    if loader_defs.code:
        import_lines.append(f"import {thds.tabularasa.loaders.util.__name__}\n")
    if schema.remote_blob_store is not None:
        import_lines.append(f"import {thds.tabularasa.schema.metaschema.__name__}\n")

    global_var_defs = []
    if schema.remote_blob_store is not None:
        global_var_defs.append(
            render_blob_store_def(schema.remote_blob_store, REMOTE_BLOB_STORE_VAR_NAME)
        )

    import_lines += type_defs.tabularasa_imports
    import_lines += loader_defs.tabularasa_imports

    imports = "".join(import_lines)
    globals_ = "\n".join(global_var_defs)
    types = "\n\n".join(type_defs.code)
    classes = "\n\n".join(table_defs)

    # loaders
    loaders = "\n\n".join(loader_defs.code)

    # module
    return autoformat(
        f"{imports}\n# {AUTOGEN_DISCLAIMER}\n\n{globals_}\n\n{types}\n\n\n{classes}\n\n{loaders}\n"
    )


def render_attrs_module(
    schema: metaschema.Schema,
    loader_defs: ty.Optional[ImportsAndCode] = None,
    use_newtypes: bool = False,
    type_constraint_comments: bool = True,
    import_external_types: bool = True,
) -> str:
    return _render_attrs_schema(
        schema,
        render_attrs_type_defs(
            schema,
            use_newtypes=use_newtypes,
            type_constraint_comments=type_constraint_comments,
            import_external_types=import_external_types,
        ),
        loader_defs,
    )
