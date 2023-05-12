import re
from typing import List, cast

import numpy as np
import pandas as pd
import pytest
from pydantic import AnyUrl

import thds.tabularasa.schema as schema
from thds.tabularasa.schema import metaschema


@pytest.mark.parametrize(
    "dtype,np_type",
    [
        (schema.dtypes.DType.INT8, np.int8),
        (schema.dtypes.DType.INT16, np.int16),
        (schema.dtypes.DType.INT32, np.int32),
        (schema.dtypes.DType.INT64, np.int64),
        (schema.dtypes.DType.UINT8, np.uint8),
        (schema.dtypes.DType.UINT16, np.uint16),
        (schema.dtypes.DType.UINT32, np.uint32),
        (schema.dtypes.DType.UINT64, np.uint64),
        (schema.dtypes.DType.FLOAT32, np.float32),
        (schema.dtypes.DType.FLOAT64, np.float64),
        (schema.dtypes.DType.DATE, np.dtype("datetime64[ns]")),
        (schema.dtypes.DType.DATETIME, np.dtype("datetime64[ns]")),
        (schema.dtypes.DType.BOOL, np.bool_),
    ],
)
def test_dtype_numpy_type(dtype: schema.dtypes.DType, np_type: np.dtype):
    assert dtype.pandas() == np_type


@pytest.mark.parametrize(
    "dtype,np_type",
    [
        (schema.dtypes.DType.INT8, pd.Int8Dtype()),
        (schema.dtypes.DType.INT16, pd.Int16Dtype()),
        (schema.dtypes.DType.INT32, pd.Int32Dtype()),
        (schema.dtypes.DType.INT64, pd.Int64Dtype()),
        (schema.dtypes.DType.UINT8, pd.UInt8Dtype()),
        (schema.dtypes.DType.UINT16, pd.UInt16Dtype()),
        (schema.dtypes.DType.UINT32, pd.UInt32Dtype()),
        (schema.dtypes.DType.UINT64, pd.UInt64Dtype()),
        (schema.dtypes.DType.FLOAT32, np.float32),
        (schema.dtypes.DType.FLOAT64, np.float64),
        (schema.dtypes.DType.DATE, np.dtype("datetime64[ns]")),
        (schema.dtypes.DType.DATETIME, np.dtype("datetime64[ns]")),
        (schema.dtypes.DType.BOOL, pd.BooleanDtype()),
    ],
)
def test_dtype_nullable_numpy_type(dtype: schema.dtypes.DType, np_type: np.dtype):
    assert dtype.pandas(nullable=True) == np_type


match_constraint = schema.constraints.MatchesRegex(matches=re.compile("foobar"))

len_constraint = schema.constraints.LenLessThan(len_lt=5)  # type: ignore[call-arg]
# I can't figure out this type issue - the pydantic plugin is enabled.

int_enum_constraint = schema.constraints.EnumConstraint(enum=[1, 2])

build_options = metaschema.BuildOptions(
    attrs=True,
    sqlite_data=False,
    sqlite_interface=True,
    pandas=True,
    pyarrow=True,
    package_data_dir="data/derived/",
    transient_data_dir="data/transient/",
    derived_code_submodule="loaders",
    sqlite_db_path="reference_data.db",
    table_docs_dir="docs/source/tables/",
    type_docs_path="docs/source/types.md",
    repo_url=cast(AnyUrl, "https://github.com/TrilliantHealth/ds-monorepo/tree/main/"),
    use_newtypes=True,
    type_constraint_comments=True,
    validate_transient_tables=True,
    require_typing_extensions=True,
    package_data_file_size_limit=12345,
)

bad_schema = dict(
    build_options=build_options,
    tables=dict(
        foo=dict(
            columns=[
                dict(
                    name="col1",
                    type=dict(
                        custom="missingtype",
                    ),
                    doc="blah blah",  # missing type ref
                ),
                dict(
                    name="col2",
                    type=dict(
                        custom="type1",
                    ),
                    doc="doc doc go",
                ),
            ],
            doc="just a table",
            primary_key=["notcol1", "notcol2"],  # missing columns
            constraints=[dict(unique=["col2"]), dict(unique=["notcol1", "col1"])],  # missing columns
            indexes=[["col1", "col2"], ["col1", "notcol1", "notcol2"]],  # missing columns
            dependencies=dict(package="notapackage", filename="bar"),  # package not installed
        ),
        bar=dict(
            columns=[
                dict(
                    name="col3",
                    type=dict(
                        custom="type3",
                    ),
                    doc="Smokey, this is not 'nam, this is bowling. There are rules.",
                    nullable=False,
                ),
                dict(name="col4", type="bool", nullable=True, doc="Mark it zero!"),
                dict(
                    name="col5",
                    type=dict(
                        custom="type2",
                    ),
                    doc="It's a league game, Smokey",
                ),
                dict(name="col4", type="bool", nullable=True, doc="Calmer than you are"),
            ],
            doc="just another table",
            primary_key=["col3", "col4"],
            constraints=[
                dict(unique=["col3", "col3", "col4", "col4"]),  # repeated columns
                dict(unique=["col4", "col5"]),
            ],
            indexes=[["col4"], ["notcol"]],  # missing columns
            dependencies=dict(
                package="pytest", filename="baz"
            ),  # resource doesn't exist in known package
        ),
        baz=dict(
            columns=[
                dict(name="col1", type="int32", doc="nothing"),
                dict(name="collection", type=dict(values="str"), doc="doc"),
            ],
            doc="empty column tuples and bad table ref",
            primary_key=[],  # empty column lists
            indexes=[[], ["collection"]],  # collection-valued column in index
            constraints=[
                dict(unique=[]),
                dict(unique=["collection"]),
            ],  # collection-valued column in constraint
            # bad resource refs and preprocessor
            # build-time table depends on run-time table
            dependencies=dict(preprocessor="foo.bar.baz", reference=["quux", "spam"], local=["foo"]),
        ),
        spam=dict(
            columns=[
                dict(name="col1", type=dict(type="str", constraints=[dict(len_ge=0)]), doc="nothing")
            ],
            inherit_schema=dict(
                tables=["baz", "eggs", "bam"],
                exclude_columns=["col1", "col2"],
                update_docs={"col3": "not a column"},
                update_nullability={"col4": True},
            ),
            doc="run-time-installed table with deps",
            build_time_installed=False,
            dependencies=dict(preprocessor="builtins.__import__", reference=["eggs"]),
        ),
        eggs=dict(
            columns=[dict(name="col1", type="int32", doc="nothing")],
            doc="build-time-installed table without deps",
            build_time_installed=True,
            dependencies=None,  # deps required for build-time tables
        ),
    ),
    types=dict(
        type1=dict(type="int32", constraints=[dict(enum=[]), len_constraint]),  # empty enum  # bad type
        type2=dict(
            type="float32",
            constraints=[
                dict(gt=0.0),
                match_constraint,  # bad type
                dict(gt=1.0),  # duplicate constraint type
            ],
        ),
        type3=dict(
            type="str",
            constraints=[
                int_enum_constraint,  # bad type,
                dict(enum=["three", "four"]),  # duplicate constraint type
            ],
        ),
        external_undefined=dict(
            schema_name="notaschema",
            type_name="doesntmatter",
        ),
        external_not_loaded=dict(
            schema_name="not_a_package",
            type_name="doesntmatter",
        ),
    ),
    local_data=dict(
        dataset1=dict(package="blah", filename="notapath"),
        dataset2=dict(package="pytest", filename="baz"),  # known existent package
    ),
    external_schemas=dict(
        dict(
            not_a_package=dict(
                package="notapackage",
                derived_code_submodule="submodule",
                schema_path="not a path but it doesn't matter",
            ),
            not_a_package_resource=dict(
                package="pytest",  # known existent package
                derived_code_submodule="loaders",
                schema_path="notaresource.yaml",
            ),
            not_a_file=dict(
                schema_path="notafile.yaml",
            ),
        )
    ),
)

expected_validation_errors = [
    schema.validation.external_schema_not_found(
        "not_a_package", "notapackage", "not a path but it doesn't matter", module_not_found=True
    ),
    schema.validation.external_schema_not_found(
        "not_a_package_resource", "pytest", "notaresource.yaml", module_not_found=False
    ),
    schema.validation.external_schema_not_found(
        "not_a_file", None, "notafile.yaml", module_not_found=False
    ),
    schema.validation.missing_external_schema("external_undefined", "notaschema", failed_to_load=False),
    schema.validation.missing_external_schema(
        "external_not_loaded", "not_a_package", failed_to_load=True
    ),
    schema.validation.missing_custom_type("col1", 0, "foo", "missingtype"),
    schema.validation.missing_cols_in_constraint("foo", "primary key", ["notcol1", "notcol2"]),
    schema.validation.missing_cols_in_constraint("foo", "unique constraint", ["notcol1"], 1),
    schema.validation.missing_cols_in_constraint("foo", "index", ["notcol1", "notcol2"], 1),
    schema.validation.package_not_installed("foo", "tabular file source for table", "notapackage"),
    schema.validation.repeated_cols_in_table("bar", ["col4"]),
    schema.validation.repeated_cols_in_constraint("bar", "unique constraint", ["col3", "col4"], 0),
    schema.validation.missing_cols_in_constraint("bar", "index", ["notcol"], 1),
    schema.validation.resource_doesnt_exist("bar", "tabular file source for table", "pytest", "baz"),
    schema.validation.empty_enum("type1", 0),
    schema.validation.constraint_doesnt_apply("type1", 1, len_constraint, "int32"),
    schema.validation.constraint_doesnt_apply("type2", 1, match_constraint, "float32"),
    schema.validation.repeated_constraint_type("type2", schema.constraints.GreaterThan),
    schema.validation.constraint_doesnt_apply("type3", 0, int_enum_constraint, "str"),
    schema.validation.repeated_constraint_type("type3", schema.constraints.EnumConstraint),
    schema.validation.empty_column_tuple("baz", "primary key"),
    schema.validation.empty_column_tuple("baz", "index", 0),
    schema.validation.empty_column_tuple("baz", "unique constraint", 0),
    schema.validation.missing_local_data_spec("baz", "quux", "reference"),
    schema.validation.missing_local_data_spec("baz", "foo", "raw"),
    schema.validation.preprocessor_not_importable(
        "baz", "foo.bar.baz", ModuleNotFoundError("No module named 'foo'")
    ),
    schema.validation.index_invalid_for_collection_type("baz", "collection", 1),
    schema.validation.uniqueness_check_invalid_for_collection_type("baz", "collection", 1),
    schema.validation.missing_inherited_table("spam", "bam"),
    schema.validation.missing_inherited_column("spam", "col2", ["baz", "eggs"], "exclusion"),
    schema.validation.missing_inherited_column("spam", "col3", ["baz", "eggs"], "docstring update"),
    schema.validation.missing_inherited_column("spam", "col4", ["baz", "eggs"], "nullability update"),
    schema.validation.run_time_table_is_build_time_dependency("spam"),
    schema.validation.dependencies_required_for_build_time_tables("eggs"),
    schema.validation.package_not_installed("dataset1", "local data specification", "blah"),
    schema.validation.resource_doesnt_exist("dataset2", "local data specification", "pytest", "baz"),
]


@pytest.fixture()
def schema_validation_errors() -> List[schema.validation.ErrorMessage]:
    raw_schema = metaschema._RawSchema(**bad_schema)  # type: ignore[arg-type]
    errors, external_schemas = schema.validation.validation_errors(
        raw_schema,
        require_data_resources=True,
        require_preprocessors=True,
    )
    return errors


@pytest.fixture()
def inheritance_schema() -> metaschema._RawSchema:
    return metaschema._RawSchema(
        build_options=build_options,
        tables=dict(
            foo=metaschema._RawTable(
                columns=[
                    metaschema._RawColumn(
                        name="col1", type=schema.dtypes.DType.INT8, doc="col1 in foo", nullable=True
                    ),
                    metaschema._RawColumn(
                        name="col2", type=schema.dtypes.DType.INT16, doc="col2 in foo", nullable=False
                    ),
                ],
                doc="table foo",
            ),
            bar=metaschema._RawTable(
                inherit_schema=metaschema.InheritanceSpec(
                    tables=["foo"],
                    exclude_columns={"col1"},
                    update_docs=dict(col2="col2 in bar"),
                ),
                columns=[
                    metaschema._RawColumn(
                        name="col3", type=schema.dtypes.DType.FLOAT32, doc="col3 in bar", nullable=True
                    ),
                ],
                doc="table bar",
            ),
            baz=metaschema._RawTable(
                inherit_schema=metaschema.InheritanceSpec(
                    tables=["bar", "foo"],
                    update_docs=dict(col2="col2 in baz", col1="col1 in baz"),
                    update_nullability=dict(col1=False),
                ),
                columns=[
                    metaschema._RawColumn(
                        name="col3", type=schema.dtypes.DType.DATE, doc="col3 in baz", nullable=False
                    ),
                    metaschema._RawColumn(
                        name="col4", type=schema.dtypes.DType.INT32, doc="col4 in baz", nullable=True
                    ),
                ],
                doc="table baz",
            ),
        ),
    )


@pytest.mark.parametrize("error", expected_validation_errors)
def test_schema_validation_error_present(
    schema_validation_errors: List[schema.validation.ErrorMessage], error: schema.validation.ErrorMessage
):
    assert error in schema_validation_errors


def test_no_schema_validation_errors_absent(
    schema_validation_errors: List[schema.validation.ErrorMessage],
):
    assert len(schema_validation_errors) == len(expected_validation_errors), str(
        set(schema_validation_errors).difference(expected_validation_errors)
    )


def test_validation_raises():
    with pytest.raises(schema.validation.MetaschemaValidationError):
        _ = schema.validation.validate(bad_schema)


@pytest.mark.parametrize(
    "table_name,expected_columns",
    [
        (
            "bar",
            [
                # defined in bar
                metaschema._RawColumn(
                    name="col3", type=schema.dtypes.DType.FLOAT32, doc="col3 in bar", nullable=True
                ),
                # inherited from foo (col1 excluded)
                metaschema._RawColumn(
                    name="col2", type=schema.dtypes.DType.INT16, doc="col2 in bar", nullable=False
                ),
            ],
        ),
        (
            "baz",
            [
                # defined in baz
                metaschema._RawColumn(
                    name="col3", type=schema.dtypes.DType.DATE, doc="col3 in baz", nullable=False
                ),
                metaschema._RawColumn(
                    name="col4", type=schema.dtypes.DType.INT32, doc="col4 in baz", nullable=True
                ),
                # inherited from bar
                metaschema._RawColumn(
                    name="col2", type=schema.dtypes.DType.INT16, doc="col2 in baz", nullable=False
                ),
                # inherited from foo
                metaschema._RawColumn(
                    name="col1", type=schema.dtypes.DType.INT8, doc="col1 in baz", nullable=False
                ),
            ],
        ),
    ],
)
def test_table_inheritance(
    table_name: str,
    inheritance_schema: metaschema._RawSchema,
    expected_columns: List[metaschema._RawColumn],
):
    table = inheritance_schema.tables[table_name]
    actual_columns = table.resolve_inherited_columns(inheritance_schema)
    assert actual_columns == expected_columns
