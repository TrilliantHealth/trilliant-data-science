import contextlib
import datetime
import difflib
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
from typing import ContextManager, Dict, List, Optional, Tuple, Type

import numpy as np
import pandas as pd
import pandera as pa
import pkg_resources
import pytest

from thds.tabularasa.data_dependencies.build import write_package_data_tables
from thds.tabularasa.data_dependencies.sqlite import populate_sqlite_db
from thds.tabularasa.data_dependencies.tabular import PandasCSVLoader
from thds.tabularasa.loaders.util import AttrsParquetLoader, PandasParquetLoader
from thds.tabularasa.schema import load_schema
from thds.tabularasa.schema.compilation import (
    render_attrs_module,
    render_attrs_sqlite_schema,
    render_pandera_module,
    render_sql_schema,
)
from thds.tabularasa.schema.metaschema import Schema

d = datetime.date.fromisoformat
dt = datetime.datetime.fromisoformat

TupleTestCases = Dict[str, List[Tuple]]

TEST_PACKAGE = "tests"


def line_diff(generated_source: str, expected_source: str) -> str:
    """For pretty-printing informative diffs between generated and expected code in tests of code
    generation features"""
    return "\n".join(difflib.ndiff(generated_source.splitlines(), expected_source.splitlines()))


@dataclass(frozen=True)
class ReferenceDataTestCase:
    schema_path: str
    dataframes: Dict[str, pd.DataFrame]
    tuples: Dict[str, List[Tuple]]
    schema_warning_pattern: Optional[str] = None
    package: str = TEST_PACKAGE
    schema: Optional[Schema] = None
    pandas_source: Optional[str] = None
    pandas_module: Optional[ModuleType] = None
    attrs_source: Optional[str] = None
    attrs_module: Optional[ModuleType] = None
    sqlite_table_source: Optional[str] = None
    sqlite_index_source: Optional[str] = None
    attrs_sqlite_source: Optional[str] = None
    attrs_sqlite_module: Optional[ModuleType] = None

    def load_schema(self):
        context: ContextManager
        if self.schema_warning_pattern:
            context = pytest.warns(UserWarning, match=self.schema_warning_pattern)
        else:
            context = contextlib.nullcontext()

        with context:
            schema = load_schema(
                self.package,
                self.schema_path,
                require_data_resources=True,
                require_preprocessors=True,
            )

        return self.with_attrs(schema=schema)

    def derive_data(self):
        assert self.schema is not None
        if self.schema.build_options.package_data_dir:
            write_package_data_tables(
                self.schema,
                package=self.package,
                check_hash=True,
                output_data_dir=self.schema.build_options.package_data_dir,
                transient_data_dir=self.schema.build_options.package_data_dir,
                validate_transient_tables=True,
            )

    def with_attrs(self, **attrs):
        current_attrs = self.__dict__.copy()
        current_attrs.update(attrs)
        return ReferenceDataTestCase(**current_attrs)

    @property
    def schema_prefix(self) -> str:
        return self.schema_path[: -len("_schema.yml")]

    @property
    def schema_name(self) -> str:
        return self.schema_prefix.split("/")[-1]

    @property
    def sqlite_db_path(self) -> str:
        assert self.schema is not None
        assert self.schema.build_options.package_data_dir is not None
        return f"{self.schema.build_options.package_data_dir}{self.schema_prefix.split('/')[-1]}.sqlite"

    @property
    def sqlite_db_conn(self) -> sqlite3.Connection:
        db_path = Path(pkg_resources.resource_filename(self.package, self.sqlite_db_path)).absolute()
        return sqlite3.connect(str(db_path))

    def _resource_path(self, filename_suffix: str) -> str:
        return f"{self.package.replace('.', '/')}/{self.schema_prefix}{filename_suffix}"

    def _load_text_file_contents(self, filename_suffix: str) -> str:
        filename = self._resource_path(filename_suffix)
        with open(filename, "r") as f:
            return f.read()

    @property
    def expected_pandas_source(self) -> str:
        return self._load_text_file_contents("_pandas.py_")

    @property
    def expected_attrs_source(self) -> str:
        return self._load_text_file_contents("_attrs.py_")

    @property
    def expected_attrs_sqlite_source(self) -> str:
        return self._load_text_file_contents("_attrs_sqlite.py_")

    @property
    def expected_sqlite_table_source(self) -> str:
        return self._load_text_file_contents("_table.sql")

    @property
    def expected_sqlite_index_source(self) -> str:
        return self._load_text_file_contents("_index.sql")

    def compile_sqlite_source(self):
        assert self.schema is not None
        sql_table_source, sql_index_source = render_sql_schema(self.schema)
        return self.with_attrs(
            sqlite_table_source=sql_table_source,
            sqlite_index_source=sql_index_source,
        )

    def compile_pandas_source(self):
        assert self.schema is not None
        pandas_source = render_pandera_module(
            self.schema,
            package=self.package,
        )
        return self.with_attrs(pandas_source=pandas_source)

    def load_pandas_module(self):
        assert self.pandas_source is not None
        pandas_module = ModuleType("autogen_pandas_test")
        exec(self.pandas_source, pandas_module.__dict__)
        return self.with_attrs(pandas_module=pandas_module)

    def compile_attrs_source(self):
        assert self.schema is not None
        attrs_source = render_attrs_module(
            self.schema,
            package=self.package,
        )
        return self.with_attrs(attrs_source=attrs_source)

    def load_attrs_module(self):
        assert self.attrs_source is not None
        attrs_module = ModuleType("autogen_attrs_test")
        exec(self.attrs_source, attrs_module.__dict__)
        return self.with_attrs(attrs_module=attrs_module)

    def populate_sqlite_db(self):
        assert self.schema is not None
        if self.schema.build_options.package_data_dir is not None:
            populate_sqlite_db(
                self.schema,
                db_package=self.package,
                db_path=self.sqlite_db_path,
                data_package=self.package,
                data_dir=self.schema.build_options.package_data_dir,
                transient_data_dir=self.schema.build_options.package_data_dir,
                check_hash=False,
            )

    def compile_attrs_sqlite_source(self):
        assert self.schema is not None
        # we specify attrs_module_name=None here to skip rendering the import of attrs classes
        attrs_sqlite_source = render_attrs_sqlite_schema(
            self.schema,
            package=self.package,
            db_path=self.sqlite_db_path if self.schema.build_options.package_data_dir else "",
            attrs_module_name=None,
        )
        return self.with_attrs(attrs_sqlite_source=attrs_sqlite_source)

    def load_attrs_sqlite_module(self):
        assert self.attrs_sqlite_source is not None
        assert self.attrs_module is not None
        attrs_sqlite_module = ModuleType("autogen_attrs_sqlite_test")
        # we pass in the namespace of the attrs_module for the 3rd, 'locals' arg, since we can't render
        # the import of the attrs classes from a pre-existing module on the path
        attrs_sqlite_module.__dict__.update(self.attrs_module.__dict__)
        exec(self.attrs_sqlite_source, attrs_sqlite_module.__dict__)
        return self.with_attrs(attrs_sqlite_module=attrs_sqlite_module)

    def pandas_csv_loader_for(self, table_name: str) -> PandasCSVLoader:
        assert self.schema is not None
        assert self.pandas_module is not None
        table = self.schema.tables[table_name]
        pandera_schema = self.pandera_schema_for(table_name)
        return PandasCSVLoader(table, pandera_schema)

    def pandera_schema_for(self, table_name: str) -> pa.DataFrameSchema:
        assert self.schema is not None
        assert self.pandas_module is not None
        table = self.schema.tables[table_name]
        pandera_schema: pa.DataFrameSchema = self.pandas_module.__dict__[
            f"{table.snake_case_name}_schema"
        ]
        return pandera_schema

    def pandas_parquet_loader_for(self, table_name: str) -> PandasParquetLoader:
        assert self.schema is not None
        assert self.pandas_module is not None
        table = self.schema.tables[table_name]
        pandas_loader: PandasParquetLoader = self.pandas_module.__dict__[f"load_{table.snake_case_name}"]
        return pandas_loader

    def dynamic_pandas_parquet_loader_for(self, table_name: str) -> PandasParquetLoader:
        assert self.schema is not None
        assert self.pandas_module is not None
        assert self.schema.build_options.package_data_dir is not None
        pandas_loader: PandasParquetLoader = PandasParquetLoader.from_schema_table(
            self.schema.tables[table_name],
            package=self.package,
            data_dir=self.schema.build_options.package_data_dir,
            derive_schema=True,
        )
        return pandas_loader

    def attrs_class_for(self, table_name: str) -> Type:
        assert self.attrs_module is not None
        assert self.schema is not None
        table = self.schema.tables[table_name]
        attrs_type: Type = self.attrs_module.__dict__[table.class_name]
        return attrs_type

    def attrs_loader_for(self, table_name: str) -> AttrsParquetLoader:
        assert self.schema is not None
        attrs_loader: AttrsParquetLoader = self.attrs_module.__dict__[f"load_{table_name}"]
        return attrs_loader

    def attrs_sqlite_loader_for(self, table_name: str):
        assert self.schema is not None
        assert self.attrs_sqlite_module is not None
        db_cls = getattr(self.attrs_sqlite_module, "SQLiteLoader", None)
        if db_cls is None:
            return None
        db = db_cls()
        table = self.schema.tables[table_name]
        return getattr(db, table.snake_case_name, None)


int_dataframes = dict(
    ints=pd.DataFrame(
        dict(
            col1=[1, -2, 3, -4],
            col2=[1, None, 3, None],
            col3=[1, 2, 3, 4],
            col4=[10, 8, 6, 4],
        ),
    )
    .astype(dict(col1=np.int64, col2=pd.Int32Dtype(), col3=np.uint16, col4=np.uint8))
    .set_index("col3"),
)

string_dataframes = dict(
    strings=pd.DataFrame(
        dict(
            uppercase=["ONE", "TWO", None, "THREE", "FOUR"],
            enum=[None, "foo", "bar", "baz", None],
            lowercase=["complex is", "better", "than", "", "complicated"],
            empty=[""] * 5,
        ),
    )
    .astype(
        dict(
            uppercase=pd.StringDtype(),
            enum=pd.CategoricalDtype(["foo", "bar", "baz"], ordered=True),
            lowercase=pd.StringDtype(),
            empty=pd.StringDtype(),
        )
    )
    .set_index("lowercase")
    .sort_index(),
)

bool_dataframes = dict(
    bools=pd.DataFrame(
        dict(
            boolean=[True, False, True, False, True],
            trinary=[True, False, None, True, False],
        )
    ).astype(
        dict(boolean=np.bool_, trinary=pd.BooleanDtype()),
    ),
)

date_dataframes = dict(
    dates=pd.DataFrame(
        dict(
            date1=[
                "1920-01-02",
                "1950-02-03",
                "1980-03-05",
                "2010-05-08",
                "2040-08-13",
            ],
            date2=[None, "1900-01-10", "1950-06-20", "2000-11-30", None],
            datetime1=[
                "1920-01-02T01:02:03",
                "1950-02-03T02:04:06",
                "1980-03-05T03:06:09",
                "2010-05-08T04:08:12",
                "2040-08-13T05:10:15",
            ],
            datetime2=[
                "1920-01-02T01:02:03",
                None,
                "1980-03-05T03:06:09",
                None,
                "2040-08-13T05:10:15",
            ],
        ),
    )
    .astype(
        dict(
            date1="datetime64[ns]",
            date2="datetime64[ns]",
            datetime1="datetime64[ns]",
            datetime2="datetime64[ns]",
        ),
    )
    .set_index(["date1", "datetime2"]),
)

array_dataframes = dict(
    arrays=pd.DataFrame(
        dict(
            pk=[0, 1, 2, 3, 4],
            int_array=[
                [-2, -1, 0, 1, 2],
                None,
                [2, 3, 5, 7, 11, 13],
                None,
                [1, 1, 2, 3, 5, 8, 13],
            ],
            nested_string_array=[
                None,
                [["FOO"], ["BAR"], ["BAZ"]],
                [[], ["1"], ["1", "2"]],
                [],
                None,
            ],
            nested_date_array=[
                [],
                [[]],
                [[], [d("2020-01-01")]],
                [[], [d("2020-01-01")], [d("2020-02-01"), d("2020-02-02")]],
                [
                    [],
                    [d("2020-01-01")],
                    [d("2020-02-01"), d("2020-02-02")],
                    [d("2020-03-01"), d("2020-03-02"), d("2020-03-03")],
                ],
            ],
        ),
    )
    .astype(
        dict(pk="int64", int_array="object", nested_string_array="object"),
    )
    .set_index("pk"),
)

mapping_dataframes = dict(
    mappings=pd.DataFrame(
        dict(
            pk=[0, 1, 2, 3, 4],
            string_to_int_mapping=[None, {"1": 1}, None, {"2": 2}, None],
            int_to_string_mapping=[{1: "one"}, None, {2: "two"}, None, {3: "three"}],
        ),
    )
    .astype(dict(pk="int16", string_to_int_mapping="object", int_to_string_mapping="object"))
    .set_index("pk"),
    nested_mappings=pd.DataFrame(
        dict(
            string_to_int_array=[
                {},
                {"one": [1]},
                {"two": [1, 2]},
                {"three": [1, 2, 3]},
            ],
            date_to_datetime_array=[
                None,
                {d("1900-01-01"): []},
                {d("1950-02-02"): [dt("1950-02-02T01:02:03")]},
                {
                    d("2000-03-03"): [
                        dt("2000-03-03T01:02:03"),
                        dt("2000-03-03T04:05:06"),
                    ]
                },
            ],
        ),
    ).astype(dict(string_to_int_array="object")),
)

dependency_dataframes = dict(
    sequences=pd.DataFrame(
        dict(
            n=[1, 2, 3, 4, 5, 6],
            fibonacci=[1, 2, 3, 5, 8, 13],
            square=[1, 4, 9, 16, 25, 36],
            triangular=[1, 3, 6, 10, 15, 21],
            is_fibonacci=[True, True, True, False, True, False],
        ),
    )
    .astype(
        dict(
            n="int16",
            fibonacci="int16",
            square="int16",
            triangular="int16",
            is_fibonacci="bool",
        )
    )
    .set_index("n"),
)

int_tuples: TupleTestCases = dict(
    ints=[(1, 1, 1, 10), (-2, None, 2, 8), (3, 3, 3, 6), (-4, None, 4, 4)],
)

string_tuples: TupleTestCases = dict(
    strings=[
        ("ONE", None, "complex is", ""),
        ("TWO", "foo", "better", ""),
        (None, "bar", "than", ""),
        ("THREE", "baz", "", ""),
        ("FOUR", None, "complicated", ""),
    ],
)

bool_tuples: TupleTestCases = dict(
    bools=[(True, True), (False, False), (True, None), (False, True), (True, False)],
)

date_tuples: TupleTestCases = dict(
    dates=[
        (d("1920-01-02"), None, dt("1920-01-02T01:02:03"), dt("1920-01-02T01:02:03")),
        (d("1950-02-03"), d("1900-01-10"), dt("1950-02-03T02:04:06"), None),
        (
            d("1980-03-05"),
            d("1950-06-20"),
            dt("1980-03-05T03:06:09"),
            dt("1980-03-05T03:06:09"),
        ),
        (d("2010-05-08"), d("2000-11-30"), dt("2010-05-08T04:08:12"), None),
        (d("2040-08-13"), None, dt("2040-08-13T05:10:15"), dt("2040-08-13T05:10:15")),
    ],
)

array_tuples: TupleTestCases = dict(
    arrays=[
        (0, [-2, -1, 0, 1, 2], None, []),
        (1, None, [["FOO"], ["BAR"], ["BAZ"]], [[]]),
        (2, [2, 3, 5, 7, 11, 13], [[], ["1"], ["1", "2"]], [[], [d("2020-01-01")]]),
        (3, None, [], [[], [d("2020-01-01")], [d("2020-02-01"), d("2020-02-02")]]),
        (
            4,
            [1, 1, 2, 3, 5, 8, 13],
            None,
            [
                [],
                [d("2020-01-01")],
                [d("2020-02-01"), d("2020-02-02")],
                [d("2020-03-01"), d("2020-03-02"), d("2020-03-03")],
            ],
        ),
    ],
)

mapping_tuples: TupleTestCases = dict(
    mappings=[
        (0, None, {1: "one"}),
        (1, {"1": 1}, None),
        (2, None, {2: "two"}),
        (3, {"2": 2}, None),
        (4, None, {3: "three"}),
    ],
    nested_mappings=[
        ({}, None),
        ({"one": [1]}, {d("1900-01-01"): []}),
        ({"two": [1, 2]}, {d("1950-02-02"): [dt("1950-02-02T01:02:03")]}),
        (
            {"three": [1, 2, 3]},
            {d("2000-03-03"): [dt("2000-03-03T01:02:03"), dt("2000-03-03T04:05:06")]},
        ),
    ],
)

dependency_tuples: TupleTestCases = dict(
    sequences=[
        (1, 1, 1, 1, True),
        (2, 2, 4, 3, True),
        (3, 3, 9, 6, True),
        (4, 5, 16, 10, False),
        (5, 8, 25, 15, True),
        (6, 13, 36, 21, False),
    ],
)


# preprocessors for dependent tables


def preprocess_int_sequences_table(
    package_tables: Dict[str, pd.DataFrame], adls_files=None, local_files=None
):
    fibonaccis = package_tables["fibonaccis"]
    squares = package_tables["squares"]
    sequences = fibonaccis.join(squares)
    sequences["triangular"] = sequences.index * (sequences.index + 1) // 2
    sequences["is_fibonacci"] = sequences.index.isin(sequences["fibonacci"])
    return sequences


@pytest.fixture(
    scope="session",
    params=[
        pytest.param(
            ReferenceDataTestCase("data/bool_schema.yml", bool_dataframes, bool_tuples),
            id="bool_schema",
        ),
        pytest.param(
            ReferenceDataTestCase(
                "data/int_schema.yml",
                int_dataframes,
                int_tuples,
                schema_warning_pattern=r"Discarding type 'string_enum' which is referenced in no table",
            ),
            id="int_schema",
        ),
        pytest.param(
            ReferenceDataTestCase("data/string_schema.yml", string_dataframes, string_tuples),
            id="string_schema",
        ),
        pytest.param(
            ReferenceDataTestCase("data/date_schema.yml", date_dataframes, date_tuples),
            id="date_schema",
        ),
        pytest.param(
            ReferenceDataTestCase(
                "data/array_schema.yml",
                array_dataframes,
                array_tuples,
            ),
            id="array_schema",
        ),
        pytest.param(
            ReferenceDataTestCase(
                "data/array_schema.yml",
                array_dataframes,
                array_tuples,
                # schema_warning_pattern=r"Array elements with custom type .*'short_upper'.* cannot
                # currently be validated",
            ),
            id="array_schema",
        ),
        pytest.param(
            ReferenceDataTestCase(
                "data/mapping_schema.yml",
                mapping_dataframes,
                mapping_tuples,
                schema_warning_pattern=r"Mapping (?:keys|values) with custom type "
                r".*(?:short_upper|small_int).* cannot currently be validated",
            ),
            id="mapping_schema",
        ),
        pytest.param(
            ReferenceDataTestCase(
                "data/dependency_schema.yml",
                dependency_dataframes,
                dependency_tuples,
                schema_warning_pattern=r"Table 'dangling_transient' is marked as transient but has no "
                r"downstream dependencies",
            ),
            id="dependency_schema",
        ),
        pytest.param(
            ReferenceDataTestCase(
                "data/types_schema.yml",
                {},
                {},
            ),
            id="mixed_types_schema",
        ),
        pytest.param(
            ReferenceDataTestCase(
                "data/types2_schema.yml",
                {},
                {},
            ),
            id="mixed_types_schema_2",
        ),
    ],
)
def test_case(request):
    yield request.param


# load and validate the schema - all other tests and fixtures are downstream of this


@pytest.fixture(scope="session")
def test_case_with_schema(test_case: ReferenceDataTestCase):
    yield test_case.load_schema()


# code generation fixtures - depend only on a validated schema


@pytest.fixture(scope="module")
def test_case_with_compiled_sqlite(test_case_with_schema: ReferenceDataTestCase):
    yield test_case_with_schema.compile_sqlite_source()


@pytest.fixture(scope="session")
def test_case_with_compiled_pandas(test_case_with_schema: ReferenceDataTestCase):
    yield test_case_with_schema.compile_pandas_source()


@pytest.fixture(scope="session")
def test_case_with_compiled_attrs(test_case_with_schema: ReferenceDataTestCase):
    yield test_case_with_schema.compile_attrs_source()


@pytest.fixture(scope="session")
def test_case_with_compiled_attrs_sqlite(test_case_with_schema: ReferenceDataTestCase):
    yield test_case_with_schema.compile_attrs_sqlite_source()


# data derivation/packaging fixtures - depend only on a validated schema


@pytest.fixture(scope="session")
def test_case_with_derived_data(test_case_with_schema: ReferenceDataTestCase):
    # need schema loaded to derive package data
    test_case_with_schema.derive_data()
    yield test_case_with_schema


@pytest.fixture(scope="session")
def test_case_with_sqlite_db(test_case_with_derived_data: ReferenceDataTestCase):
    # need derived data to populate the database
    test_case_with_derived_data.populate_sqlite_db()
    yield test_case_with_derived_data


# executable module compilation fixtures - depend on generated code and data;
# data accessor code should be executable for loading data


@pytest.fixture(scope="session")
def test_case_with_pandas_module(
    test_case_with_compiled_pandas: ReferenceDataTestCase,
    test_case_with_derived_data: ReferenceDataTestCase,
):
    # need pandas accessor code to dynamically load the associated module,
    # and derived package data to run the accessor code in the module
    yield test_case_with_compiled_pandas.load_pandas_module()


@pytest.fixture(scope="session")
def test_case_with_attrs_module(
    test_case_with_compiled_attrs: ReferenceDataTestCase,
    test_case_with_derived_data: ReferenceDataTestCase,
):
    # need attrs accessor code to dynamically load the associated module,
    # and derived package data to run the accessor code in the module
    yield test_case_with_compiled_attrs.load_attrs_module()


@pytest.fixture(scope="session")
def test_case_with_attrs_sqlite_module(
    test_case_with_compiled_attrs_sqlite: ReferenceDataTestCase,
    test_case_with_attrs_module: ReferenceDataTestCase,
    test_case_with_sqlite_db: ReferenceDataTestCase,
):
    # need attrs sqlite accessor code to dynamically load the associated module,
    # need compiled attrs module to have attrs classes available at code eval time
    # and need populated sqlite database to run the accessor code in the module
    test_case = test_case_with_attrs_module.with_attrs(
        attrs_sqlite_source=test_case_with_compiled_attrs_sqlite.attrs_sqlite_source
    )
    yield test_case.load_attrs_sqlite_module()
