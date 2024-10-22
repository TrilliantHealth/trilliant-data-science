import io
import itertools
import os
from collections import Counter
from functools import lru_cache
from pathlib import Path
from typing import Any, Collection, Dict, List, Mapping, Optional, Set, Tuple, Type, Union, cast

import networkx as nx
import pkg_resources
import yaml
from _warnings import warn

from .. import git_util
from .constraints import AnyColumnConstraint, EnumConstraint
from .dtypes import DType
from .files import ADLSDataSpec, LocalDataSpec, TabularFileSource
from .metaschema import (
    JSON,
    AnonCustomType,
    ArrayType,
    Column,
    CustomType,
    ExternalCustomType,
    ExternalTypeRef,
    MappingType,
    RawDataDependencies,
    Schema,
    Table,
    _CustomTypeRef,
    _RawArrayType,
    _RawColumn,
    _RawMappingType,
    _RawSchema,
    _RawTable,
)
from .util import Identifier, import_func, predecessor_graph

ErrorMessage = str


class MetaschemaValidationError(ValueError):
    def __init__(self, errors: List[ErrorMessage]):
        self.errors = errors

    def __str__(self):
        return "\n".join(self.errors)


def empty_column_tuple(table_name: str, kind: str, index: Optional[int] = None):
    index_expr = "" if index is None else f"at index {index} "
    return f"Table '{table_name}' {kind} {index_expr}is empty"


def repeated_cols_in_table(table_name: str, repeated_cols: Collection[str]) -> ErrorMessage:
    return f"Table '{table_name}' has repeated column names {sorted(repeated_cols)}"


def missing_cols_in_constraint(
    table_name: str, constraint_type: str, missing_cols: Collection[str], index: Optional[int] = None
) -> ErrorMessage:
    index_ = "" if index is None else f" (index {index})"
    return (
        f"Table '{table_name}' {constraint_type}{index_} references columns {sorted(missing_cols)}"
        f" which are undefined"
    )


def repeated_cols_in_constraint(
    table_name: str, constraint_type: str, repeated_cols: Collection[str], index: Optional[int] = None
) -> ErrorMessage:
    index_ = "" if index is None else f" (index {index})"
    return f"Table '{table_name}' {constraint_type}{index_} has repeated columns {sorted(repeated_cols)}"


def uniqueness_check_invalid_for_collection_type(
    table_name: str,
    column_name: str,
    constraint_index: Optional[int] = None,
):
    index_spec = "" if constraint_index is None else f" at index {constraint_index}"
    return (
        f"Cannot check uniqueness for collection-valued column '{column_name}' in table "
        f"'{table_name}'; occurred in table constraint{index_spec}"
    )


def index_invalid_for_collection_type(
    table_name: str,
    column_name: str,
    constraint_index: Optional[int] = None,
) -> ErrorMessage:
    index_spec = "primary key" if constraint_index is None else f"index at index {constraint_index}"
    return (
        f"Cannot use collection-valued column '{column_name}' in table '{table_name}' in an index; "
        f"occurred in {index_spec}"
    )


def missing_custom_type(column_name: str, index: int, table_name: str, type_name: str) -> ErrorMessage:
    return (
        f"Column '{column_name}' (index {index}) of table '{table_name}' references custom type "
        f"'{type_name}' which does not exist"
    )


def missing_inherited_table(table_name: str, inherited_table_name: str) -> ErrorMessage:
    return (
        f"Table '{table_name}' references inherited table '{inherited_table_name}' which does not exist"
    )


def missing_inherited_column(
    table_name: str, inherited_column: str, inherited_tables: Collection[str], reason: str
) -> ErrorMessage:
    return (
        f"Table '{table_name}' references column '{inherited_column}' for {reason} in its inheritance "
        f"specification, which is present in none of the inherited tables: {list(inherited_tables)}"
    )


def missing_remote_data_spec(table_name: str, remote_data_ref_name: str) -> ErrorMessage:
    return (
        f"Table '{table_name}' references remote data dependency '{remote_data_ref_name}' which "
        f"does not exist"
    )


def missing_local_data_spec(
    table_name: str, local_data_ref_name: str, local_data_type: str
) -> ErrorMessage:
    return (
        f"Table '{table_name}' references '{local_data_type}' data dependency "
        f"'{local_data_ref_name}' which does not exist"
    )


def missing_external_schema(
    type_name: str, external_schema_name: str, failed_to_load: bool
) -> ErrorMessage:
    return (
        f"Type '{type_name}' references external schema '{external_schema_name}' "
        f"which {'failed to load' if failed_to_load else 'is undefined'}"
    )


def missing_external_type(
    type_name: str, external_schema_name: str, external_type_name: str
) -> ErrorMessage:
    return (
        f"Type '{type_name}' references type '{external_type_name}' which isn't present in external schema "
        f"'{external_schema_name}' (possibly dropped as not referenced)"
    )


def source_name_defined_for_derived_table(table_name: str, column_name: str) -> ErrorMessage:
    return (
        f"Table '{table_name}' is derived but defines a source name for column {column_name}; "
        f"derived tables should be written in the same schema in which they are read"
    )


def constraint_doesnt_apply(
    type_name: str, index: int, constraint: AnyColumnConstraint, dtype: str
) -> ErrorMessage:
    return (
        f"Constraint {constraint} (index {index}) of custom type '{type_name}' doesn't apply to "
        f"dtype '{dtype}'"
    )


def dependencies_required_for_build_time_tables(table_name: str) -> ErrorMessage:
    return (
        f"Table '{table_name}' is marked as build-time-installed but has no dependencies; "
        f"build-time-installed tables must specify data dependencies"
    )


def repeated_constraint_type(type_name: str, constraint_type: Type) -> ErrorMessage:
    return f"Constraint type {constraint_type} is repeated for custom type '{type_name}'"


def empty_enum(type_name: str, index: int) -> ErrorMessage:
    return f"Constraint for type '{type_name}' (index {index}) is empty enum"


def resource_doesnt_exist(
    resource_name: str, resource_type: str, package_name: Optional[str], file_name: str
) -> ErrorMessage:
    package_addendum = f" in package '{package_name}'" if package_name else ""
    return (
        f"Resource for {resource_type} '{resource_name}' doesn't exist{package_addendum}"
        f"at path '{file_name}'"
    )


def resource_order_mismatch(
    resource_name: str,
    resource_type: str,
    package_name: Optional[str],
    resource_paths: Set[str],
    ordered_paths: Set[str],
) -> ErrorMessage:
    package_addendum = f" from the package '{package_name}'" if package_name else ""
    return (
        f"The set of files in the resource for {resource_type} '{resource_name}'{package_addendum} "
        f"does not equal the set of files specified in the resource's order: "
        f"{resource_paths} != {ordered_paths}"
    )


def ordered_resource_is_not_dir(
    resource_name: str,
    resource_type: str,
    package_name: Optional[str],
    file_name: str,
    ordered_paths: Set[str],
) -> ErrorMessage:
    package_addendum = f" in package '{package_name}'" if package_name else ""
    return (
        f"Package resource for {resource_type} '{resource_name}'{package_addendum}"
        f"at path '{file_name}' is not a directory but the resource has an order set: {ordered_paths}"
    )


def package_not_installed(resource_name: str, resource_type: str, package_name: str) -> ErrorMessage:
    return f"Package '{package_name}' in {resource_type} '{resource_name}' is not installed"


def preprocessor_not_importable(
    table_name: str, preprocessor_path: str, exception: Exception
) -> ErrorMessage:
    return (
        f"Preprocessor function path {preprocessor_path} for table {table_name} is not importable: "
        f"{exception!r}"
    )


def preprocessor_not_callable(
    table_name: str, preprocessor_path: str, exception: Exception
) -> ErrorMessage:
    return (
        f"Preprocessor function path {preprocessor_path} for table {table_name} does not reference"
        f" a function: {exception!r}"
    )


def external_schema_invalid(schema_name: str) -> ErrorMessage:
    return f"External schema '{schema_name}' failed to validate"


def external_schema_not_found(
    schema_name: str, package_name: Optional[str], schema_path: str, module_not_found: bool
) -> ErrorMessage:
    package = "" if package_name is None else f" in package {package_name}"
    return (
        f"External schema '{schema_name}' was not loaded at path '{schema_path}'{package}; "
        f"{'module' if module_not_found else 'file'} not found"
    )


def run_time_table_is_build_time_dependency(table_name: str) -> ErrorMessage:
    return (
        f"Run-time-installed table '{table_name}' is a transitive dependency of "
        f"build-time-installed tables"
    )


def dependency_graph_not_a_dag(cycle: List[Tuple[Any, Any]]) -> ErrorMessage:
    return graph_not_a_dag("Data dependency", cycle)


def inheritance_graph_not_a_dag(cycle: List[Tuple[Any, Any]]) -> ErrorMessage:
    return graph_not_a_dag("Table inheritance", cycle)


def graph_not_a_dag(kind: str, cycle: List[Tuple[Any, Any]]) -> ErrorMessage:
    nodes = [*(e[0] for e in cycle), cycle[-1][1]]
    cycle_str = " -> ".join(map(repr, nodes))
    return f"{kind} graph is not a DAG; example cycle: {cycle_str}"


def _validate_unique_column_names(table: _RawTable, tablename: str) -> List[ErrorMessage]:
    errors = []
    colnames = {c.snake_case_name for c in table.columns}
    if len(colnames) < len(table.columns):
        counts = Counter(c.snake_case_name for c in table.columns)
        duped = {n for n, c in counts.items() if c > 1}
        errors.append(repeated_cols_in_table(tablename, duped))

    return errors


def _validate_table_constraints(
    table: _RawTable, tablename: str, schema: _RawSchema
) -> List[ErrorMessage]:
    errors = []
    colnames = {c.name: c for c in table.resolve_inherited_columns(schema)}

    def repeated(xs: Optional[Collection]) -> List:
        if xs is None:
            return []
        counts = Counter(xs)
        return [x for x, n in counts.items() if n > 1]

    for constraint_kind, column_tuples in [
        ("unique constraint", ((i, c.unique) for i, c in enumerate(table.constraints))),
        ("index", enumerate(table.indexes)),
        ("primary key", [] if table.primary_key is None else [(None, table.primary_key)]),
    ]:
        for i, columns in column_tuples:
            if not len(columns):
                errors.append(empty_column_tuple(tablename, constraint_kind, i))
                continue

            missing_cols = set(columns).difference(colnames)
            if missing_cols:
                errors.append(missing_cols_in_constraint(tablename, constraint_kind, missing_cols, i))

            repeated_cols = repeated(columns)
            if repeated_cols:
                errors.append(repeated_cols_in_constraint(tablename, constraint_kind, repeated_cols, i))

            for colname in columns:
                column = colnames.get(colname)
                if column is not None and isinstance(
                    column.type, (_RawArrayType, _RawMappingType, ArrayType, MappingType)
                ):
                    if constraint_kind == "unique constraint":
                        errors.append(
                            uniqueness_check_invalid_for_collection_type(tablename, colname, i)
                        )
                    else:
                        errors.append(index_invalid_for_collection_type(tablename, colname, i))

    return errors


def _validate_column_types(
    table: _RawTable,
    tablename: str,
    custom_types: Collection[Identifier],
) -> List[ErrorMessage]:
    errors = []
    for i, column in enumerate(table.columns):
        for refname in column.custom_type_refs:
            if refname not in custom_types:
                errors.append(missing_custom_type(column.name, i, tablename, refname))

    return errors


def _validate_table_inheritance(
    table: _RawTable,
    tablename: str,
    schema: _RawSchema,
):
    inheritance = table.inherit_schema
    if inheritance is None:
        return []

    errors = []
    inherited_table_names = []
    inherited_tables = []
    for inherited_table_name in inheritance.tables:
        if inherited_table_name not in schema.tables:
            errors.append(missing_inherited_table(tablename, inherited_table_name))
        elif inherited_table_name in inherited_table_names:
            warn(
                f"Table '{inherited_table_name}' is repeated in inherited table list for table "
                f"'{tablename}'"
            )
        else:
            inherited_table_names.append(inherited_table_name)
            inherited_tables.append(schema.tables[inherited_table_name])

    heritable_column_names = {
        c.name for table in inherited_tables for c in table.resolve_inherited_columns(schema)
    }
    defined_column_names = {c.name for c in table.columns}

    for column_set, kind in [
        (inheritance.columns, "inclusion"),
        (inheritance.update_docs, "docstring update"),
        (inheritance.update_nullability, "nullability update"),
        (inheritance.update_source_name, "source name update"),
    ]:
        for column_name in column_set:
            if column_name not in heritable_column_names:
                errors.append(
                    missing_inherited_column(tablename, column_name, inherited_table_names, kind)
                )
            defined_explicitly = column_name in defined_column_names
            excluded = (
                bool(inheritance.columns)
                and (column_name not in inheritance.columns)
                and kind != "inclusion"
            )
            if defined_explicitly or excluded:
                reference_type = " and ".join(
                    s
                    for s, condition in [
                        ("not marked for inclusion", excluded),
                        ("defined explicitly", defined_explicitly),
                    ]
                    if condition
                )
                addendum = (
                    "; it will not be present in the resulting table schema"
                    if excluded and not defined_explicitly
                    else ""
                )
                warn(
                    f"Column '{column_name}' is marked for {kind} in inheritance specification for "
                    f"table '{tablename}', but also {reference_type}{addendum}"
                )

    return errors


def _validate_data_dependencies(
    table: _RawTable,
    tablename: str,
    tables: Mapping[Identifier, _RawTable],
    remote_data: Mapping[Identifier, ADLSDataSpec],
    local_data: Mapping[Identifier, LocalDataSpec],
) -> List[ErrorMessage]:
    errors = []
    if table.build_time_installed and table.dependencies is None:
        errors.append(dependencies_required_for_build_time_tables(tablename))

    if isinstance(table.dependencies, RawDataDependencies):
        for refname in table.dependencies.adls:
            if refname not in remote_data:
                errors.append(missing_remote_data_spec(tablename, refname))

        for refname in table.dependencies.reference:
            if refname not in tables:
                errors.append(missing_local_data_spec(tablename, refname, "reference"))

        for refname in table.dependencies.local:
            if refname not in local_data:
                errors.append(missing_local_data_spec(tablename, refname, "raw"))

        for column in table.columns:
            if column.source_name is not None:
                errors.append(source_name_defined_for_derived_table(tablename, column.name))

    return errors


def _validate_type_constraints(type_: AnonCustomType, typename: str) -> List[ErrorMessage]:
    errors = []
    for i, constraint in enumerate(type_.constraints):
        if not constraint.applies_to(type_.type):
            errors.append(constraint_doesnt_apply(typename, i, constraint, type_.type.value))
        if isinstance(constraint, EnumConstraint):
            if not constraint.enum:
                errors.append(empty_enum(typename, i))

    constraint_type_counts = Counter(map(type, type_.constraints))
    repeated_constraint_types = [t for t, c in constraint_type_counts.items() if c > 1]
    if repeated_constraint_types:
        errors.extend(repeated_constraint_type(typename, t) for t in repeated_constraint_types)

    return errors


def _validate_external_type_ref(
    type_: ExternalTypeRef,
    external_schemas: Mapping[Identifier, Schema],
    typename: str,
    failed_external_schemas: Set[str],
) -> List[ErrorMessage]:
    errors = []
    if type_.schema_name not in external_schemas:
        errors.append(
            missing_external_schema(
                typename, type_.schema_name, type_.schema_name in failed_external_schemas
            )
        )
    else:
        external_schema = external_schemas[type_.schema_name]
        if type_.type_name not in external_schema.types:
            errors.append(missing_external_type(typename, type_.schema_name, type_.type_name))

    return errors


def _validate_local_data_resource(
    package: Optional[str], data_path: str, resource_name: str, resource_desc: str
) -> List[ErrorMessage]:
    errors: List[ErrorMessage] = []
    if package is None:
        exists = os.path.isfile(data_path) or os.path.isdir(data_path)
    else:
        try:
            exists = pkg_resources.resource_exists(package, data_path)
        except ModuleNotFoundError:
            errors.append(package_not_installed(resource_name, resource_desc, package))
            exists = True

    if not exists:
        errors.append(resource_doesnt_exist(resource_name, resource_desc, package, data_path))

    return errors


def _validate_local_ordered_data_resource(
    resource: LocalDataSpec, resource_name: str, resource_desc: str
) -> List[ErrorMessage]:
    errors: List[ErrorMessage] = []
    assert resource.order is not None
    ordered_paths = set(resource.order)
    if resource.is_dir:
        files = set()
        for filename in resource.list_dir():
            files.add(os.path.basename(filename))
        if files != ordered_paths:
            errors.append(
                resource_order_mismatch(
                    resource_name, resource_desc, resource.package, files, ordered_paths
                )
            )
    else:
        errors.append(
            ordered_resource_is_not_dir(
                resource_name, resource_desc, resource.package, resource.filename, ordered_paths
            )
        )
    return errors


def _validate_preprocessor(table: _RawTable, tablename: str) -> List[ErrorMessage]:
    errors: List[ErrorMessage] = []
    if not isinstance(table.dependencies, RawDataDependencies):
        return errors

    funcpath = table.dependencies.preprocessor
    try:
        import_func(funcpath)
    except (ImportError, AttributeError) as e:
        errors.append(preprocessor_not_importable(tablename, funcpath, e))
    except TypeError as e:
        errors.append(preprocessor_not_callable(tablename, funcpath, e))

    return errors


def _validate_dependency_dag(schema: _RawSchema) -> List[ErrorMessage]:
    errors: List[ErrorMessage] = []
    full_graph = Schema.dependency_dag(schema, lambda table: True)  # type: ignore

    def to_nodeset(predicate):
        return set(
            table._graph_ref(tablename) for tablename, table in schema.tables.items() if predicate(table)
        )

    build_time_tables = to_nodeset(lambda table: table.build_time_installed)
    run_time_tables = to_nodeset(lambda table: table.run_time_installed)
    transient_tables = to_nodeset(lambda table: table.transient)

    if not nx.is_directed_acyclic_graph(full_graph):
        cycle = nx.find_cycle(full_graph)
        errors.append(dependency_graph_not_a_dag(cycle))

    # no runtime-installed table should be a recursive dependency of any build-time-installed table
    build_time_graph = predecessor_graph(full_graph, build_time_tables)
    for ref in build_time_graph:
        if ref in run_time_tables:
            errors.append(run_time_table_is_build_time_dependency(str(ref)))

    # transient tables should have successors; however, this is not an error, just a warning that such
    # tables will be silently ignored at build time
    for ref in transient_tables:
        if not list(full_graph.successors(ref)):
            warn(
                f"Table '{ref}' is marked as transient but has no downstream dependencies; it will not "
                f"be computed in builds"
            )

    return errors


def _validate_inheritance_dag(schema: _RawSchema) -> List[ErrorMessage]:
    full_graph = schema.inheritance_dag()
    errors = []
    if not nx.is_directed_acyclic_graph(full_graph):
        cycle = nx.find_cycle(full_graph)
        errors.append(inheritance_graph_not_a_dag(cycle))
    return errors


def _resolve_typeref(
    dtype: Union[DType, AnonCustomType, _CustomTypeRef, _RawArrayType, _RawMappingType],
    custom_types: Mapping[Identifier, CustomType],
) -> Union[DType, AnonCustomType, CustomType, ArrayType, MappingType]:
    if isinstance(dtype, _CustomTypeRef):
        return custom_types[dtype.custom]
    elif isinstance(dtype, _RawArrayType):
        if isinstance(dtype.values, (AnonCustomType, _CustomTypeRef)):
            warn(f"Array elements with custom type {dtype.values} cannot currently be validated")
        return ArrayType(values=_resolve_typeref(dtype.values, custom_types))
    elif isinstance(dtype, _RawMappingType):
        if isinstance(dtype.keys, (AnonCustomType, _CustomTypeRef)):
            warn(f"Mapping keys with custom type {dtype.keys} cannot currently be validated")
        if isinstance(dtype.values, (AnonCustomType, _CustomTypeRef)):
            warn(f"Mapping values with custom type {dtype.values} cannot currently be validated")
        return MappingType(
            keys=cast(
                Union[DType, CustomType, AnonCustomType], _resolve_typeref(dtype.keys, custom_types)
            ),
            values=_resolve_typeref(dtype.values, custom_types),
        )
    else:
        return dtype


def _resolve_column_typerefs(
    column: _RawColumn, custom_types: Mapping[Identifier, CustomType]
) -> Column:
    return Column(
        name=column.name,
        type=_resolve_typeref(column.type, custom_types),
        nullable=column.nullable,
        doc=column.doc,
        source_name=column.source_name,
        na_values=column.na_values,
    )


def distinct_indexes(table: _RawTable, table_name: str) -> List[Tuple[str, ...]]:
    indexes = []
    for index in table.indexes:
        if index == table.primary_key:
            warn(
                f"Table {table_name} has its primary key re-defined as an index: {table.primary_key}; "
                f"discarding"
            )
        elif index in indexes:
            warn(f"Table {table_name} has a duplicate definition of index {index}; discarding")
        else:
            indexes.append(index)

    return indexes


def _load_external_schema(
    schema_name: str,
    package: Optional[str],
    schema_path: str,
    git_ref: Optional[str] = None,
) -> Tuple[Optional[Schema], List[ErrorMessage]]:
    errors = []
    external_schema: Optional[Schema] = None
    try:
        external_schema = load_schema(
            package,
            schema_path,
            require_data_resources=False,
            require_preprocessors=False,
            git_ref=git_ref,
        )
    except ModuleNotFoundError:
        errors.append(
            external_schema_not_found(schema_name, package, schema_path, module_not_found=True)
        )
    except FileNotFoundError:
        errors.append(
            external_schema_not_found(schema_name, package, schema_path, module_not_found=False)
        )
    except MetaschemaValidationError:
        errors.append(external_schema_invalid(schema_name))

    return external_schema, errors


def validation_errors(
    raw_schema: _RawSchema,
    require_external_schemas: bool = True,
    require_data_resources: bool = False,
    require_preprocessors: bool = False,
    git_ref: Optional[str] = None,
) -> Tuple[List[ErrorMessage], Mapping[str, Schema]]:
    errors = _validate_inheritance_dag(raw_schema)
    bad_inheritance_graph = bool(errors)

    # load external schemas
    external_schemas = {}
    failed_external_schemas = set()
    if require_external_schemas:
        for schema_name, schema_ref in raw_schema.external_schemas.items():
            external_schema, load_errors = _load_external_schema(
                schema_name,
                schema_ref.package,
                schema_ref.schema_path,
                git_ref=git_ref,
            )
            if load_errors:
                errors.extend(load_errors)
                failed_external_schemas.add(schema_name)
            else:
                assert external_schema is not None
                external_schemas[schema_name] = external_schema

    # verify all column name refs
    for tablename, table in raw_schema.tables.items():
        errors.extend(_validate_unique_column_names(table, tablename))
        if not bad_inheritance_graph or table.inherit_schema is None:
            # this check involves resolving inherited columns, so we skip it if the inheritance graph is
            # badly formed
            errors.extend(_validate_table_constraints(table, tablename, raw_schema))

        errors.extend(_validate_column_types(table, tablename, raw_schema.types))
        errors.extend(
            _validate_data_dependencies(
                table, tablename, raw_schema.tables, raw_schema.remote_data, raw_schema.local_data
            )
        )
        if not bad_inheritance_graph and table.inherit_schema is not None:
            errors.extend(_validate_table_inheritance(table, tablename, raw_schema))
        if require_data_resources and isinstance(table.dependencies, TabularFileSource):
            errors.extend(
                _validate_local_data_resource(
                    table.dependencies.package,
                    table.dependencies.filename,
                    tablename,
                    "tabular file source for table",
                )
            )
        if require_preprocessors:
            errors.extend(_validate_preprocessor(table, tablename))

    if require_data_resources:
        for resourcename, local_resource in raw_schema.local_data.items():
            errors.extend(
                _validate_local_data_resource(
                    local_resource.package,
                    local_resource.filename,
                    resourcename,
                    "local data specification",
                )
            )
            if local_resource.order:
                errors.extend(
                    _validate_local_ordered_data_resource(
                        local_resource,
                        resourcename,
                        "local data specification",
                    )
                )
                for i, order_path in enumerate(local_resource.order):
                    errors.extend(
                        _validate_local_data_resource(
                            local_resource.package,
                            "/".join([local_resource.filename, order_path]),
                            resourcename,
                            f"local data order [{i}] specification",
                        )
                    )

    for typename, dtype in raw_schema.types.items():
        if isinstance(dtype, AnonCustomType):
            errors.extend(_validate_type_constraints(dtype, typename))
        elif require_external_schemas:
            errors.extend(
                _validate_external_type_ref(dtype, external_schemas, typename, failed_external_schemas)
            )

    errors.extend(_validate_dependency_dag(raw_schema))

    return errors, external_schemas


def validate(
    json: Dict,
    require_data_resources: bool = False,
    require_preprocessors: bool = False,
    git_ref: Optional[str] = None,
) -> Schema:
    # low-level pydantic validation happens here
    raw_schema = _RawSchema(**json)
    # higher-level semantic validation happens here
    errors, external_schemas = validation_errors(
        raw_schema,
        require_external_schemas=True,
        require_data_resources=require_data_resources,
        require_preprocessors=require_preprocessors,
        git_ref=git_ref,
    )
    if errors:
        raise MetaschemaValidationError(errors)

    named_custom_types: Dict[Identifier, Union[CustomType, ExternalCustomType]] = {}
    referenced_custom_types = set(
        itertools.chain.from_iterable(table.custom_type_refs for table in raw_schema.tables.values())
    )
    for name, t in raw_schema.types.items():
        if not raw_schema.build_options.render_all_types and name not in referenced_custom_types:
            warn(f"Discarding type {name!r} which is referenced in no table")
        else:
            if isinstance(t, ExternalTypeRef):
                external_schema = external_schemas[t.schema_name]
                external_type = external_schema.types[t.type_name]
                schema_ref = raw_schema.external_schemas[t.schema_name]
                schema_ref.derived_code_submodule = external_schema.build_options.derived_code_submodule
                named_custom_types[name] = external_type.from_external(schema_ref, name)
            else:
                named_custom_types[name] = t.with_name(name)

    for name, spec in raw_schema.remote_data.items():
        non_version_controlled_paths = [p for p in spec.paths if p.md5 is None]
        if non_version_controlled_paths:
            warn(
                f"Remote data specification '{name}' has {len(non_version_controlled_paths)} "
                "paths with no specified hash; build consistency cannot be guaranteed for any "
                "tables depending on these"
            )

    return Schema(
        build_options=raw_schema.build_options,
        tables={
            tablename: Table(
                name=tablename,
                columns=[
                    _resolve_column_typerefs(column, named_custom_types)
                    for column in table.resolve_inherited_columns(raw_schema)
                ],
                constraints=table.constraints,
                primary_key=table.primary_key,
                indexes=distinct_indexes(table, tablename),
                doc=table.doc,
                dependencies=table.dependencies,
                md5=table.md5,
                transient=table.transient,
                build_time_installed=table.build_time_installed,
            )
            for tablename, table in raw_schema.tables.items()
        },
        types=named_custom_types,
        external_schemas=raw_schema.external_schemas,
        remote_data=raw_schema.remote_data,
        remote_blob_store=raw_schema.remote_blob_store,
        local_data=raw_schema.local_data,
    )


@lru_cache(None)  # singleton
def load_schema(
    package: Optional[str],
    schema_path: str,
    require_data_resources: bool = False,
    require_preprocessors: bool = False,
    git_ref: Optional[str] = None,
) -> Schema:
    if git_ref is None:
        if package is None:
            with open(schema_path, "r") as f:
                json: JSON = yaml.safe_load(f)
        else:
            with pkg_resources.resource_stream(package, schema_path) as f:
                json = yaml.safe_load(f)

    else:
        abspath = (
            Path(schema_path)
            if package is None
            else Path(pkg_resources.resource_filename(package, str(schema_path)))
        )
        contents = git_util.blob_contents(abspath, git_ref)
        json = yaml.safe_load(io.BytesIO(contents))

    return validate(
        json,
        require_data_resources=require_data_resources,
        require_preprocessors=require_preprocessors,
        git_ref=git_ref,
    )
