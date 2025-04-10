import datetime
import itertools
import typing
from collections import defaultdict
from typing import (
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Mapping,
    NamedTuple,
    Optional,
    Sequence,
    Set,
    Tuple,
    Type,
    Union,
    cast,
)
from warnings import warn

import networkx as nx
import numpy as np
import pandera as pa
import pyarrow
import typing_extensions
from pandas.core.dtypes import base as pd_dtypes
from pydantic import AnyUrl, BaseModel, Extra, Field

from thds.tabularasa.schema.files import FileSourceMixin

from .constraints import AnyColumnConstraint, EnumConstraint
from .dtypes import AnyDtype, DType
from .files import ADLSDataSpec, LocalDataSpec, RemoteBlobStoreSpec, TabularFileSource
from .util import (
    DocumentedMixin,
    DottedIdentifier,
    EnumList,
    HexStr,
    Identifier,
    NonEmptyStr,
    PathStr,
    predecessor_graph,
    render_dtype,
    snake_case,
    snake_to_title,
)

JSON = Dict[str, Union[Dict[str, Any], List[Any], int, float, str, bool, None]]

IdTuple = Tuple[Identifier, ...]


class AnonCustomType(DocumentedMixin):
    type: DType
    constraints: List[AnyColumnConstraint] = Field(min_items=1)

    def with_name(self, name: Identifier) -> "CustomType":
        return CustomType(
            type=self.type, constraints=self.constraints, doc=self.doc, markup=self.markup, name=name
        )

    @property
    def python(self) -> Type:
        enum = self.enum
        if enum is not None:
            return cast(Type, typing_extensions.Literal[tuple(enum.enum)])
        return self.type.python

    def python_type_literal(self, build_options: "BuildOptions", builtin: bool = False) -> str:
        if builtin:
            return self.type.python_type_literal(build_options=build_options, builtin=builtin)
        else:
            return self.python_type_def_literal(build_options=build_options)

    def python_type_def_literal(self, build_options: "BuildOptions"):
        enum = self.enum
        if enum is not None:
            module = typing_extensions if build_options.require_typing_extensions else typing
            values = ", ".join(map(repr, enum.enum))
            return f"{module.__name__}.Literal[{values}]"
        else:
            return self.type.python_type_literal(builtin=False, build_options=build_options)

    @property
    def parquet(self) -> pyarrow.DataType:
        return self.type.parquet

    @property
    def enum(self) -> Optional[EnumConstraint]:
        return next((c for c in self.constraints if isinstance(c, EnumConstraint)), None)

    def attrs_required_imports(self, build_options: "BuildOptions") -> Set[str]:
        imports = self.type.attrs_required_imports(build_options=build_options)
        extra = None
        if self.enum is not None:
            # render as literal
            # extensions is technically not a std lib but it's easier to account for this way
            extra = "typing_extensions" if build_options.require_typing_extensions else "typing"
        elif build_options.use_newtypes:
            # render as newtype
            extra = "typing"
        if extra is not None:
            imports.add(extra)
        return imports

    @property
    def custom_type_refs(self) -> Iterator[Identifier]:
        yield from self.type.custom_type_refs

    @property
    def comment(self) -> Optional[str]:
        comments = list(filter(None, (con.comment_expr() for con in self.constraints)))
        if comments:
            return "; ".join(comments)
        return None


class CustomType(AnonCustomType, extra=Extra.forbid):
    name: Identifier

    @property
    def class_name(self) -> str:
        return snake_to_title(self.name)

    def python_type_literal(self, build_options: "BuildOptions", builtin: bool = False) -> str:
        if builtin:
            return self.type.python_type_literal(build_options=build_options, builtin=builtin)
        else:
            return self.class_name

    def python_type_def_literal(self, build_options: "BuildOptions"):
        literal = super().python_type_def_literal(build_options)
        if build_options.use_newtypes and self.enum is None:
            return f'typing.NewType("{self.class_name}", {literal})'
        else:
            return literal

    @property
    def custom_type_refs(self) -> Iterator[Identifier]:
        yield self.name
        yield from super().custom_type_refs

    def from_external(
        self, ref: "ExternalSchemaRef", new_name: Optional[str]
    ) -> Union["CustomType", "ExternalCustomType"]:
        if ref.package is None or ref.derived_code_submodule is None:
            warn(
                f"Either package or derived_code_submodule not specified for external type ref "
                f"'{new_name}' originally named '{self.name}'; definition will be duplicated rather "
                f"than imported"
            )
            return self

        return ExternalCustomType(
            type=self.type,
            constraints=self.constraints,
            name=new_name or self.name,
            external_name=self.name,
            package=ref.package,
            derived_code_submodule=ref.derived_code_submodule,
        )


class ExternalCustomType(CustomType, extra=Extra.forbid):
    package: DottedIdentifier
    derived_code_submodule: DottedIdentifier
    external_name: Identifier

    @property
    def module_path(self) -> str:
        return f"{self.package}.{self.derived_code_submodule}.attrs"

    @property
    def external_class_name(self) -> str:
        return snake_to_title(self.external_name)

    @property
    def import_spec(self) -> Tuple[str, str]:
        old_name = self.external_class_name
        new_name = self.class_name
        return self.module_path, old_name if old_name == new_name else f"{old_name} as {new_name}"

    def attrs_required_imports(self, build_options: "BuildOptions") -> Set[str]:
        if build_options.import_external_types:
            return set()
        else:
            return super().attrs_required_imports(build_options)


class _CustomTypeRef(BaseModel, extra=Extra.forbid):
    custom: Identifier

    def __str__(self):
        return repr(self.custom)

    @property
    def custom_type_refs(self) -> Iterator[Identifier]:
        yield self.custom


class ExternalSchemaRef(BaseModel, extra=Extra.forbid):
    schema_path: str
    package: Optional[DottedIdentifier] = None
    derived_code_submodule: Optional[DottedIdentifier] = None


class ExternalTypeRef(BaseModel, extra=Extra.forbid):
    schema_name: Identifier
    type_name: Identifier


class _ComplexBaseType(BaseModel, extra=Extra.forbid):
    @property
    def sqlite(self) -> str:
        return "JSON"

    @property
    def enum(self) -> Optional[EnumConstraint]:
        return None

    def pandas(
        self,
        nullable: bool = False,
        index: bool = False,
        enum: Optional[EnumList] = None,
        ordered: bool = False,
    ):
        return np.dtype("object")


class _RawArrayType(_ComplexBaseType):
    values: Union[DType, _CustomTypeRef, AnonCustomType, "_RawArrayType", "_RawMappingType"]

    @property
    def custom_type_refs(self) -> Iterator[Identifier]:
        yield from self.values.custom_type_refs


class ArrayType(_RawArrayType):
    values: Union[DType, CustomType, AnonCustomType, "ArrayType", "MappingType"]

    @property
    def python(self) -> Type[List]:
        return List[self.values.python]  # type: ignore

    @property
    def parquet(self) -> pyarrow.DataType:
        return pyarrow.list_(self.values.parquet)

    def attrs_required_imports(self, build_options: "BuildOptions") -> Set[str]:
        return {"typing", *self.values.attrs_required_imports(build_options=build_options)}

    def python_type_literal(self, build_options: "BuildOptions", builtin: bool = False):
        return f"typing.List[{self.values.python_type_literal(build_options=build_options, builtin=builtin)}]"


class _RawMappingType(_ComplexBaseType, extra=Extra.forbid):
    keys: Union[DType, _CustomTypeRef, AnonCustomType]
    values: Union[DType, _CustomTypeRef, AnonCustomType, "_RawArrayType", "_RawMappingType"]

    @property
    def custom_type_refs(self) -> Iterator[Identifier]:
        yield from self.keys.custom_type_refs
        yield from self.values.custom_type_refs


class MappingType(_RawMappingType, extra=Extra.forbid):
    keys: Union[DType, CustomType, AnonCustomType]
    values: Union[DType, CustomType, AnonCustomType, "ArrayType", "MappingType"]

    @property
    def python(self) -> Type[Dict]:
        return Dict[self.keys.python, self.values.python]  # type: ignore

    @property
    def parquet(self) -> pyarrow.DataType:
        return pyarrow.map_(self.keys.parquet, self.values.parquet)

    def attrs_required_imports(self, build_options: "BuildOptions") -> Set[str]:
        return {
            "typing",
            *self.keys.attrs_required_imports(build_options),
            *self.values.attrs_required_imports(build_options),
        }

    def python_type_literal(self, build_options: "BuildOptions", builtin: bool = False):
        return (
            f"typing.Dict[{self.keys.python_type_literal(build_options=build_options, builtin=builtin)}, "
            f"{self.values.python_type_literal(build_options=build_options, builtin=builtin)}]"
        )


class UniqueColumnsConstraint(BaseModel, extra=Extra.forbid):
    unique: IdTuple

    @property
    def sqlite(self) -> str:
        return f'UNIQUE ({", ".join(self.unique)})'

    @staticmethod
    def make_pandera_check_expr(unique: IdTuple) -> str:
        return f"pa.{pa.Check.__name__}.unique_across_columns({list(unique)!r})"

    def pandera_check_expr(self) -> str:
        return self.make_pandera_check_expr(self.unique)

    @staticmethod
    def make_pandera_check(unique: IdTuple) -> pa.Check:
        return pa.Check.unique_across_columns(list(unique))

    def pandera_check(self) -> pa.Check:
        return self.make_pandera_check(self.unique)


# this allows the recursive types above
_RawArrayType.update_forward_refs()
_RawMappingType.update_forward_refs()
ArrayType.update_forward_refs()
MappingType.update_forward_refs()


# Remote data specifications - these are fetched at build time for preprocessing and packaging


class RawDataDependencies(DocumentedMixin):
    preprocessor: DottedIdentifier
    reference: List[Identifier] = Field(default_factory=list)
    adls: List[Identifier] = Field(default_factory=list)
    local: List[Identifier] = Field(default_factory=list)


# Raw types; these are parsed from the yaml and converted to the rich types below by resolving
# references, and checking constraints


class _RawColumn(BaseModel, extra=Extra.forbid):
    name: Identifier
    type: Union[DType, _CustomTypeRef, AnonCustomType, _RawArrayType, _RawMappingType]
    nullable: bool = False
    doc: NonEmptyStr
    source_name: Optional[str] = None
    na_values: Optional[Set[str]] = None

    def with_attrs(
        self,
        *,
        name: Optional[Identifier] = None,
        nullable: Optional[bool] = None,
        doc: Optional[NonEmptyStr] = None,
        source_name: Optional[str] = None,
    ):
        cls = type(self)
        return cls(
            name=self.name if name is None else name,
            type=self.type,
            nullable=self.nullable if nullable is None else nullable,
            doc=self.doc if doc is None else doc,
            source_name=self.source_name if source_name is None else source_name,
        )

    @property
    def snake_case_name(self) -> str:
        return snake_case(self.name)

    @property
    def custom_type_refs(self) -> Iterator[Identifier]:
        yield from self.type.custom_type_refs


class InheritanceSpec(BaseModel, extra=Extra.forbid):
    """Specification for columns in a table which inherits columns from other tables (usually transient
    tables which are then used in joins or filtered to a subset). The `tables` attribute is a list of
    tables to inherit columns from in order of precedence. The `columns` attribute is an optional list
    of columns to include from any of the tables. When absent, all columns from all tables are included.
    """

    tables: Sequence[Identifier]
    columns: Set[Identifier] = Field(default_factory=set)
    update_docs: Mapping[Identifier, str] = Field(default_factory=dict)
    update_nullability: Mapping[Identifier, bool] = Field(default_factory=dict)
    update_source_name: Mapping[Identifier, str] = Field(default_factory=dict)


class _RawTable(BaseModel, extra=Extra.forbid):
    columns: Sequence[_RawColumn] = Field(default_factory=list)
    doc: NonEmptyStr
    dependencies: Optional[Union[TabularFileSource, RawDataDependencies]] = None
    inherit_schema: Optional[InheritanceSpec] = None
    constraints: List[UniqueColumnsConstraint] = Field(default_factory=list)
    primary_key: Optional[Tuple[Identifier, ...]] = None
    indexes: List[IdTuple] = Field(default_factory=list)
    md5: Optional[HexStr] = None
    # flag to indicate that a table is only defined as a dependency for other tables;
    # if true, no package data will be written and no accessor code will be generated
    transient: bool = False
    # flag to indicate that this table's data is installed at runtime
    # accessor code will still be generated, but no package data will be produced at build time
    build_time_installed: bool = True
    title: Optional[str] = None

    def resolve_inherited_columns(self, schema: "_RawSchema") -> Sequence[_RawColumn]:
        if self.inherit_schema is None:
            return self.columns
        else:
            tables_with_precedence = [
                schema.tables[name] for name in self.inherit_schema.tables if name in schema.tables
            ]
            columns = list(self.columns)
            permitted_column_names = self.inherit_schema.columns
            used_column_names = set(c.name for c in self.columns)
            for table in tables_with_precedence:
                for column in table.resolve_inherited_columns(schema):
                    if column.name not in used_column_names and (
                        not permitted_column_names or (column.name in permitted_column_names)
                    ):
                        if (
                            column.name in self.inherit_schema.update_docs
                            or column.name in self.inherit_schema.update_nullability
                            or column.name in self.inherit_schema.update_source_name
                        ):
                            column = column.with_attrs(
                                doc=self.inherit_schema.update_docs.get(column.name),
                                nullable=self.inherit_schema.update_nullability.get(column.name),
                                source_name=self.inherit_schema.update_source_name.get(column.name),
                            )
                        columns.append(column)
                        used_column_names.add(column.name)
            return columns

    @property
    def packaged(self) -> bool:
        return not self.transient

    @property
    def run_time_installed(self) -> bool:
        return not self.build_time_installed

    @property
    def custom_type_refs(self) -> Iterator[Identifier]:
        for column in self.columns:
            yield from column.custom_type_refs

    def _graph_ref(self, name: str):
        # _RawTable has no name attribute but this is needed in DAG validation prior to construction of
        # final schema
        return TransientReferenceDataRef(name) if self.transient else ReferenceDataRef(name)


class CustomStr(str):
    """These exist to allow usage of strings as nodes in the networkx computational DAG without
    collision - a local table and an ADLS resource could have the same name and not collide in the hash
    table that underlies the networkx graph."""

    _name: str

    def __eq__(self, other):
        return type(self) is type(other) and super().__eq__(other)

    def __repr__(self):
        return f"{self._name}({super().__str__()})"

    def __hash__(self):
        return hash(repr(self))


class ADLSRef(CustomStr):
    _name = "ADLS"


class LocalRef(CustomStr):
    _name = "Local"


class TabularTextFileRef(CustomStr):
    _name = "TabularTextFile"


class ReferenceDataRef(CustomStr):
    _name = "ReferenceData"


class TransientReferenceDataRef(ReferenceDataRef):
    _name = "ReferenceData"


class BuildOptions(BaseModel, extra=Extra.forbid):
    # interface
    derived_code_submodule: DottedIdentifier
    attrs: bool
    sqlite_data: bool
    sqlite_interface: bool
    pandas: bool
    pyarrow: bool
    # interface options
    type_constraint_comments: bool = True
    validate_transient_tables: bool = True
    # set this to true if you want to generate code that's compatible with python 3.7 and lower
    require_typing_extensions: bool = False
    # import types from external schemas, or re-render them?
    import_external_types: bool = True
    # render custom types with constraints as typing.NewType instances?
    use_newtypes: bool = True
    # boolean to override behavior of dropping types not referenced by any table;
    # allows a schema that defines only types to render source code definitions
    render_all_types: bool = False
    # data
    package_data_dir: Optional[PathStr] = None
    transient_data_dir: Optional[PathStr] = None
    sqlite_db_path: Optional[PathStr] = None
    package_data_file_size_limit: Optional[int] = None
    # docs
    repo_url: Optional[AnyUrl] = None
    table_docs_dir: Optional[str] = None
    type_docs_path: Optional[str] = None
    source_docs_path: Optional[str] = None
    curation_badge_path: Optional[str] = None


class _RawSchema(BaseModel, extra=Extra.forbid):
    tables: Mapping[Identifier, _RawTable] = Field(default_factory=dict)
    types: Mapping[Identifier, Union[AnonCustomType, ExternalTypeRef]] = Field(default_factory=dict)
    external_schemas: Mapping[Identifier, ExternalSchemaRef] = Field(default_factory=dict)
    remote_data: Mapping[Identifier, ADLSDataSpec] = Field(default_factory=dict)
    local_data: Mapping[Identifier, LocalDataSpec] = Field(default_factory=dict)
    remote_blob_store: Optional[RemoteBlobStoreSpec] = None
    build_options: BuildOptions

    def inheritance_dag(self) -> nx.DiGraph:
        dag = nx.DiGraph()
        for table_name, table in self.tables.items():
            if table.inherit_schema is not None:
                dag.add_edges_from(
                    (table_name, inherited)
                    for inherited in table.inherit_schema.tables
                    if inherited in self.tables
                )
        return dag


# Final materialized schema types; these extend the raw types and override the types of some fields to
# reflect resolution of references within the schema

ResolvedDType = Union[DType, AnonCustomType, CustomType, ArrayType, MappingType]


class Column(_RawColumn):
    type: ResolvedDType

    @property
    def dtype(self) -> Union[DType, ArrayType, MappingType]:
        return self.type.type if isinstance(self.type, (AnonCustomType, CustomType)) else self.type

    def pandas(self, index: bool = False) -> AnyDtype:
        enum = self.type.enum
        if enum is not None:
            return self.dtype.pandas(
                nullable=self.nullable, enum=enum.enum, ordered=enum.ordered, index=index
            )
        else:
            return self.dtype.pandas(nullable=self.nullable, index=index)

    def pandas_dtype_literal(self, index: bool = False) -> str:
        dtype = self.pandas(index=index)
        rendered = render_dtype(dtype)

        if index and isinstance(dtype, np.dtype) and dtype.kind in "iuf":
            return f"thds.tabularasa.compat.resolve_numeric_np_index_dtype_for_pd_version({rendered})"
            # we actually need to render these dtypes wrapped in this compat function so that we can
            # render schemas using pandas>=2.0, but they will still work with pandas<2.0

        return rendered

    @property
    def python(self) -> Type:
        return Optional[self.type.python] if self.nullable else self.type.python  # type: ignore

    def python_type_literal(self, build_options: "BuildOptions", builtin: bool = False) -> str:
        # column type literals are always within the body of a record class def, i.e. not a custom type
        # def
        literal = self.type.python_type_literal(build_options=build_options, builtin=builtin)
        return f"typing.Optional[{literal}]" if self.nullable else literal

    @property
    def header_name(self) -> str:
        if self.source_name is None:
            return self.name
        return self.source_name

    @property
    def parquet_field(self) -> pyarrow.Field:
        metadata = dict(doc=self.doc.encode())
        return pyarrow.field(self.snake_case_name, self.type.parquet, self.nullable, metadata=metadata)


class Table(_RawTable):
    # mypy prefers sequence here since we subclass the type arg from _RawColumn, and Sequence is
    # covariant
    columns: Sequence[Column]
    name: Identifier
    dependencies: Optional[Union[TabularFileSource, RawDataDependencies]]

    @property
    def unique_constraints(self) -> List[UniqueColumnsConstraint]:
        if not self.constraints:
            return []
        return [c for c in self.constraints if isinstance(c, UniqueColumnsConstraint)]

    @property
    def single_column_unique_constraints(self) -> List[Identifier]:
        return [c.unique[0] for c in self.unique_constraints if len(c.unique) == 1]

    @property
    def class_name(self) -> str:
        return snake_to_title(self.name)

    @property
    def snake_case_name(self) -> str:
        return snake_case(self.name)

    @property
    def doc_title(self) -> str:
        if self.title is None:
            return snake_to_title(self.name, separator=" ")
        else:
            return self.title

    def _attrs_required_imports(
        self, build_options: "BuildOptions", sqlite_interface: bool = False
    ) -> Set[str]:
        columns: Iterator[Column]
        if sqlite_interface:
            index_cols = self.index_columns
            # don't need type literals from std lib for custom types; can import class names
            columns = (
                column
                for column in self.columns
                if column.name in index_cols and not isinstance(column.type, CustomType)
            )
        else:
            columns = iter(self.columns)

        modules = set()
        for column in columns:
            if column.nullable and not (
                isinstance(column.type, ExternalCustomType) and build_options.import_external_types
            ):
                modules.add("typing")
            modules.update(column.type.attrs_required_imports(build_options))
        return modules

    def attrs_required_imports(self, build_options: "BuildOptions") -> Set[str]:
        return self._attrs_required_imports(build_options=build_options, sqlite_interface=False)

    def attrs_sqlite_required_imports(self, build_options: "BuildOptions") -> Set[str]:
        return self._attrs_required_imports(build_options=build_options, sqlite_interface=True)

    @property
    def parquet_schema(self) -> pyarrow.Schema:
        metadata = dict(
            doc=self.doc.encode(),
            primary_key=(
                " ".join(map(snake_case, self.primary_key)).encode() if self.primary_key else b""
            ),
        )
        return pyarrow.schema([column.parquet_field for column in self.columns], metadata=metadata)

    @property
    def parquet_casts(self) -> Dict[str, Union[np.dtype, pd_dtypes.ExtensionDtype]]:
        pk = self.primary_key or ()
        casts: Dict[str, Union[np.dtype, pd_dtypes.ExtensionDtype]] = {}

        for c in self.columns:
            dtype = c.pandas(index=c.name in pk)
            if isinstance(dtype, pd_dtypes.ExtensionDtype):
                casts[c.snake_case_name] = dtype
            elif isinstance(dtype, np.dtype) and dtype.name not in ("int32", "int64"):
                casts[c.snake_case_name] = dtype

        return casts

    @property
    def csv_na_values(self) -> Dict[str, Set[str]]:
        """Dict of column name to set of string values that should be considered null when reading a
        tabular text file. Used for `na_values` arg of `pandas.read_csv`"""
        na_values: Dict[str, Set[str]] = {}
        default_na_values = (
            self.dependencies.na_values if isinstance(self.dependencies, TabularFileSource) else None
        )
        for c in self.columns:
            if c.na_values is not None:
                na_values[c.header_name] = c.na_values
            elif c.nullable and default_na_values is not None:
                na_values[c.header_name] = default_na_values
        return na_values

    @property
    def pandera_schema(self) -> pa.DataFrameSchema:
        schema = render_pandera_schema(self, as_str=False)
        return schema  # type: ignore

    @property
    def graph_ref(self) -> ReferenceDataRef:
        """Reference to a node in the computational DAG"""
        return self._graph_ref(self.name)

    @property
    def has_indexes(self) -> bool:
        return bool(self.primary_key) or bool(self.indexes)

    @property
    def index_columns(self) -> Set[Identifier]:
        return set(itertools.chain(self.primary_key or [], itertools.chain.from_iterable(self.indexes)))


# classes for mimicking pandera schema classes, to allow the same code block to generate code and
# a true pandera schema dynamically at runtime


class _ColumnSchemaProxy(NamedTuple):
    dtype: str
    checks: Optional[List[str]]
    nullable: bool
    unique: bool


class _IndexSchemaProxy(NamedTuple):
    dtype: str
    name: Identifier
    checks: Optional[List[str]]
    nullable: bool
    unique: bool


class _MultiIndexSchemaProxy(NamedTuple):
    indexes: List[_IndexSchemaProxy]
    strict: bool


class _DataFrameSchemaProxy(NamedTuple):
    columns: Dict[Identifier, _ColumnSchemaProxy]
    index: Optional[Union[_IndexSchemaProxy, _MultiIndexSchemaProxy]]  # type: ignore
    checks: List[str]
    coerce: bool
    strict: bool
    ordered: bool


def render_pandera_schema(
    table: Table, as_str: bool
) -> Union[_DataFrameSchemaProxy, pa.DataFrameSchema]:
    column_defs: List[Tuple[str, Union[_ColumnSchemaProxy, pa.Column]]] = []
    index_defs: List[Tuple[str, Union[_IndexSchemaProxy, pa.Index]]] = []
    single_col_unique_constraints = set(table.single_column_unique_constraints)
    index_names = set() if table.primary_key is None else set(table.primary_key)
    single_col_index = len(index_names) == 1

    for column in table.columns:
        check_exprs: Optional[Union[List[str], List[pa.Check]]]
        if isinstance(column.type, (AnonCustomType, CustomType)):
            if as_str:
                check_exprs = [c.pandera_check_expr() for c in column.type.constraints]
            else:
                check_exprs = [c.pandera_check() for c in column.type.constraints]
        else:
            check_exprs = None

        if column.name in index_names:
            constructor = _IndexSchemaProxy if as_str else pa.Index
            exprlist = index_defs
            extra_kw = dict(name=column.snake_case_name)
        else:
            constructor = _ColumnSchemaProxy if as_str else pa.Column
            exprlist = column_defs  # type: ignore
            extra_kw = {}

        # always enforce that indexes are unique since they derive from primary key declarations
        # multi-index uniqueness checks have to be handled with a custom check
        unique = column.name in single_col_unique_constraints or (
            single_col_index and column.name in index_names
        )
        pandas_type: Union[str, AnyDtype]
        if as_str:
            pandas_type = column.pandas_dtype_literal(index=column.name in index_names)
        else:
            pandas_type = column.pandas(index=column.name in index_names)
        expr = constructor(
            dtype=pandas_type,
            checks=check_exprs,
            nullable=column.nullable,
            unique=unique,
            **extra_kw,
        )
        exprlist.append((column.snake_case_name, expr))

    index_def: Optional[Union[_IndexSchemaProxy, _MultiIndexSchemaProxy, pa.Index, pa.MultiIndex]]
    if index_defs:
        if len(index_defs) == 1:
            _, index_def = index_defs[0]
        else:
            constructor = _MultiIndexSchemaProxy if as_str else pa.MultiIndex
            index_def = constructor(
                indexes=[expr for _name, expr in index_defs],
                strict=True,
            )
    else:
        index_def = None

    unique_constraints = [c.unique for c in table.unique_constraints if len(c.unique) > 1]
    if len(index_names) > 1 and not any(index_names == set(u) for u in unique_constraints):
        assert table.primary_key is not None  # make mypy happy
        unique_constraints.append(table.primary_key)

    if unique_constraints:
        from thds.tabularasa.loaders.util import unique_across_columns  # noqa: F401

        # Importing the above to ensure the custom pandera check is registered.
        # Ideally, custom pandera checks would be registered in a more central location.
        df_check_exprs = [
            (
                UniqueColumnsConstraint.make_pandera_check_expr(constraint)
                if as_str
                else UniqueColumnsConstraint.make_pandera_check(constraint)
            )
            for constraint in unique_constraints
        ]
    else:
        df_check_exprs = None

    schema_cls = _DataFrameSchemaProxy if as_str else pa.DataFrameSchema
    return schema_cls(
        columns=dict(column_defs),
        index=index_def,
        checks=df_check_exprs,
        coerce=False,
        strict="filter" if table.transient else True,
        ordered=False,
    )


def is_build_time_package_table(table: Table) -> bool:
    return table.build_time_installed and table.packaged


def is_run_time_package_table(table: Table) -> bool:
    return table.run_time_installed and table.packaged


class FileSourceMeta(NamedTuple):
    # full path to the data source spec in the schema structure
    schema_path: List[str]
    name: str
    source: FileSourceMixin


class Schema(_RawSchema):
    """Processed version of a `_RawSchema` that's been passed through validation to ensure integrity of
    all references, and with names denormalized onto named objects (tables and types)"""

    tables: Mapping[Identifier, Table]
    types: Mapping[Identifier, CustomType]

    @property
    def build_time_package_tables(self) -> Iterator[Table]:
        return self.filter_tables(is_build_time_package_table)

    @property
    def run_time_package_tables(self) -> Iterator[Table]:
        return self.filter_tables(is_run_time_package_table)

    @property
    def package_tables(self) -> Iterator[Table]:
        return self.filter_tables(lambda table: table.packaged)

    @property
    def transient_tables(self) -> Iterator[Table]:
        return self.filter_tables(lambda table: table.transient)

    @property
    def computable_tables(self) -> Iterator[Table]:
        return self.filter_tables(lambda table: table.dependencies is not None)

    def filter_tables(self, predicate: Callable[[Table], bool]) -> Iterator[Table]:
        return filter(predicate, self.tables.values())

    @property
    def all_custom_type_refs(self) -> Set[Identifier]:
        """Every ref to a type that will be rendered as part of this schema"""
        if self.build_options.render_all_types:
            return set(self.types)
        else:
            return set(ref for table in self.package_tables for ref in table.custom_type_refs)

    @property
    def packaged_custom_type_refs(self) -> Set[Identifier]:
        return set(
            ref
            for ref in self.all_custom_type_refs
            if not isinstance(self.types[ref], ExternalCustomType)
        )

    @property
    def external_type_refs(self) -> Set[Identifier]:
        return set(
            ref for ref in self.all_custom_type_refs if isinstance(self.types[ref], ExternalCustomType)
        )

    @property
    def attrs_required_imports(self) -> Set[str]:
        assert self.build_options is not None, "can't generate attrs schema without `build_options`"
        # all types referenced in tables. Includes imports needed for inline-defined anonymous types
        modules = set(
            itertools.chain.from_iterable(
                t.attrs_required_imports(self.build_options) for t in self.tables.values()
            )
        )
        # all top-level defined field types. Includes imports needed for types not used in any table
        modules.update(
            itertools.chain.from_iterable(
                t.attrs_required_imports(self.build_options) for t in self.defined_types
            )
        )
        return modules

    @property
    def all_file_sources(self) -> Iterator[FileSourceMeta]:
        for table_name, table in self.tables.items():
            if isinstance(table.dependencies, FileSourceMixin):
                yield FileSourceMeta(
                    ["tables", table_name, "dependencies"], table_name, table.dependencies
                )
        sources: Mapping[str, FileSourceMixin]
        for type_name, sources in [("local_data", self.local_data), ("remote_data", self.remote_data)]:
            for source_name, source in sources.items():
                yield FileSourceMeta([type_name, source_name], source_name, source)

    def sources_needing_update(self, as_of: Optional[datetime.date] = None) -> List[FileSourceMeta]:
        as_of_ = as_of or datetime.date.today()
        return [meta for meta in self.all_file_sources if meta.source.needs_update(as_of_)]

    @property
    def external_type_imports(self) -> Dict[str, Set[str]]:
        """Mapping from qualified module name to class name or
        '<external class name> as <internal class name>' expression"""
        if not self.build_options.import_external_types:
            return {}

        imports: Dict[str, Set[str]] = defaultdict(set)
        for ref in self.external_type_refs:
            t = self.types[ref]
            # true by definition of `self.external_type_refs`
            assert isinstance(t, ExternalCustomType)
            module, import_name = t.import_spec
            imports[module].add(import_name)

        return imports

    @property
    def defined_types(self) -> List[CustomType]:
        """All field types which are defined non-anonymously in the generated attrs code for this schema"""
        referenced_custom_type_refs = set(self.packaged_custom_type_refs)
        if not self.build_options.import_external_types:
            referenced_custom_type_refs.update(self.external_type_refs)

        return [self.types[name] for name in referenced_custom_type_refs]

    def dependency_dag(
        self, table_predicate: Callable[[Table], bool] = is_build_time_package_table
    ) -> nx.DiGraph:
        """Directed graph of dependencies between all data packaging steps"""
        dag = nx.DiGraph()
        tables = set()
        for tablename, table in self.tables.items():
            # run-time-installed tables have no dependencies
            table_ref = table._graph_ref(tablename)
            if table_predicate(table):
                tables.add(table_ref)
                dag.add_node(table_ref)

            if isinstance(table.dependencies, RawDataDependencies):
                for reflist, refcls in [
                    (table.dependencies.adls, ADLSRef),
                    (table.dependencies.local, LocalRef),
                ]:
                    if reflist:
                        dag.add_edges_from((refcls(dep), table_ref) for dep in reflist)
                for table_dep in table.dependencies.reference:
                    if table_dep in self.tables:
                        ref = self.tables[table_dep]._graph_ref(table_dep)
                    else:
                        # this can't actually happen post-validation but we need it in case of a bad ref
                        # during validation
                        ref = ReferenceDataRef(table_dep)
                    dag.add_edge(ref, table_ref)
            elif isinstance(table.dependencies, TabularFileSource):
                dag.add_edge(TabularTextFileRef(tablename), table_ref)
            elif table.dependencies is None and table_predicate(table):
                warn(
                    f"Table '{tablename}' has no dependencies and can not be included in the "
                    f"computational DAG; it must be installed manually via parquet files"
                )

        if len(tables) < len(dag):
            dag = predecessor_graph(dag, tables).copy()

        return dag
