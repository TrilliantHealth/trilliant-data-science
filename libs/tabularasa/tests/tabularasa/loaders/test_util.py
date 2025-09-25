import pandas as pd
import pyarrow as pa
import pytest
from pandera import Check, DataFrameSchema
from pandera.errors import SchemaError

from thds.tabularasa.loaders.parquet_util import TypeCheckLevel, pyarrow_type_compatible
from thds.tabularasa.loaders.util import unique_across_columns  # noqa: F401


def test_unique_constraint_passes():
    df = pd.DataFrame(
        dict(
            a=[1, 2, 1, 2],
            b=["a", "a", "b", "b"],
        )
    )
    schema = DataFrameSchema(checks=[Check.unique_across_columns(["a", "b"])])

    schema.validate(df)
    schema.validate(df.set_index("a"))
    schema.validate(df.set_index(["a", "b"]))


def test_unique_constraint_fails():
    df = pd.DataFrame(
        dict(
            a=[1, 2, 3, 3],
            b=["a", "a", "b", "b"],
        )
    )
    schema = DataFrameSchema(checks=[Check.unique_across_columns(["a", "b"])])

    with pytest.raises(SchemaError):
        schema.validate(df)

    with pytest.raises(SchemaError):
        schema.validate(df.set_index("a"))


base_test_cases = [
    # exact checks
    (pa.int16(), pa.uint16(), TypeCheckLevel.exact, False),
    (pa.int8(), pa.int8(), TypeCheckLevel.exact, True),
    (pa.timestamp("s"), pa.timestamp("us"), TypeCheckLevel.exact, False),
    (pa.timestamp("s"), pa.timestamp("s"), TypeCheckLevel.exact, True),
    (pa.null(), pa.bool_(), TypeCheckLevel.exact, False),
    (pa.null(), pa.null(), TypeCheckLevel.exact, True),
    # same-kind checks
    (pa.int16(), pa.int32(), TypeCheckLevel.same_kind, True),
    (pa.int16(), pa.uint32(), TypeCheckLevel.same_kind, True),
    (pa.int16(), pa.float64(), TypeCheckLevel.same_kind, False),
    (pa.date32(), pa.float32(), TypeCheckLevel.same_kind, False),
    (pa.bool_(), pa.uint8(), TypeCheckLevel.same_kind, False),
    (pa.null(), pa.bool_(), TypeCheckLevel.same_kind, False),
    # compatible checks
    (pa.timestamp("s"), pa.timestamp("us"), TypeCheckLevel.compatible, False),
    (pa.timestamp("us"), pa.timestamp("s"), TypeCheckLevel.compatible, True),
    (pa.timestamp("s"), pa.date32(), TypeCheckLevel.compatible, True),
    (pa.date32(), pa.timestamp("s"), TypeCheckLevel.compatible, False),
    (pa.int32(), pa.float64(), TypeCheckLevel.compatible, True),
    (pa.float16(), pa.uint8(), TypeCheckLevel.compatible, False),
    (pa.bool_(), pa.int16(), TypeCheckLevel.compatible, False),
    (pa.int32(), pa.bool_(), TypeCheckLevel.compatible, False),
    (pa.null(), pa.bool_(), TypeCheckLevel.compatible, False),
]
same_names_test_cases = [
    (actual, expected, TypeCheckLevel.same_names, True) for actual, expected, _, _ in base_test_cases
]
base_test_cases.extend(same_names_test_cases)
list_test_cases = [
    # test that type compatibility at a given level applies recursively to lists
    # list fields are nullable by default, so allow a null type to pass in compatible checks
    (
        pa.list_(actual),
        pa.list_(expected),
        level,
        True if ((level <= TypeCheckLevel.compatible) and (actual == pa.null())) else result,
    )
    for actual, expected, level, result in base_test_cases
]
list_test_cases.extend(
    # test that non-nullable actual fields are fine when level < exact
    (
        pa.list_(pa.field("item", actual, nullable=False)),
        pa.list_(expected),
        level,
        result if level < TypeCheckLevel.exact else False,
    )
    for actual, expected, level, result in base_test_cases
    if actual != pa.null()
)
list_test_cases.extend(
    # test that nullable actual fields with non-nullable expected fields aren't OK
    # for any level above same_names
    (
        pa.list_(actual),
        pa.list_(pa.field("item", expected, nullable=False)),
        level,
        False if level > TypeCheckLevel.same_names else result,
    )
    for actual, expected, level, result in base_test_cases
    if expected != pa.null()
)
map_test_cases = [
    # test that type compatibility at a given level applies recursively to maps
    # map *value* fields are nullable by default, so allow a null type to pass in compatible checks
    (
        pa.map_(actual if actual != pa.null() else expected, actual),
        pa.map_(expected, expected),
        level,
        True if ((level <= TypeCheckLevel.compatible) and (actual == pa.null())) else result,
    )
    for actual, expected, level, result in base_test_cases
    if expected != pa.null()
]
struct_test_cases = [
    # test that type compatibility at a given level applies recursively to structs
    # struct field types are nullable by default, so allow a null type to pass in compatible checks
    (
        pa.struct([("foo", actual)]),
        pa.struct([("foo", expected)]),
        level,
        True if ((level <= TypeCheckLevel.compatible) and (actual == pa.null())) else result,
    )
    for actual, expected, level, result in base_test_cases
]
struct_test_cases.extend(
    # test that same_names doesn't care at all about types, only that field names match
    (pa.struct([("foo", actual)]), pa.struct([("foo", expected)]), TypeCheckLevel.same_names, True)
    for actual, expected, _, _ in base_test_cases
)
struct_test_cases.extend(
    # test that same_names always fails when the actual type is missing a name from the expected type
    (pa.struct([("bar", expected)]), pa.struct([("foo", expected)]), TypeCheckLevel.same_names, False)
    for actual, expected, _, _ in base_test_cases
)
struct_test_cases.extend(
    # test that a null type is acceptable in place of a nullable field in case of a compatible check
    (
        pa.struct([("foo", pa.null())]),
        pa.struct([pa.field("foo", expected, nullable=True)]),
        level,
        True if level <= TypeCheckLevel.compatible else expected == pa.null(),
    )
    for actual, expected, level, result in base_test_cases
)
struct_test_cases.extend(
    (
        # test that exact raises on extra columns even if types are as expected
        pa.struct([("foo", actual if level < TypeCheckLevel.exact else expected), ("bar", expected)]),
        pa.struct([("foo", expected)]),
        level,
        result if level < TypeCheckLevel.exact else False,
    )
    for actual, expected, level, result in base_test_cases
    if actual != pa.null()
)


@pytest.mark.parametrize(
    "actual,expected,level,result",
    base_test_cases + same_names_test_cases + list_test_cases + map_test_cases + struct_test_cases,
)
def test_pyarrow_type_compatible(
    actual: pa.DataType, expected: pa.DataType, level: TypeCheckLevel, result: bool
):
    assert pyarrow_type_compatible(actual, expected, level) == result
