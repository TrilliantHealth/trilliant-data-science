from typing import Callable, Union

import pyarrow
import pytest

from thds.tabularasa.schema.compilation.pyarrow import pyarrow_schema_literal, pyarrow_type_literal

PyArrow = Union[pyarrow.Schema, pyarrow.DataType]

smallint = pyarrow.uint8()
bigint = pyarrow.int64()
string = pyarrow.string()
smalldate = pyarrow.date32()
medfloat = pyarrow.float32()
bool_ = pyarrow.bool_()
timestamp = pyarrow.timestamp("s", "+00:00")
listmedint = pyarrow.list_(pyarrow.int32())
listbool = pyarrow.list_(pyarrow.bool_())
listlistbigdate = pyarrow.list_(pyarrow.list_(pyarrow.date64()))
mapintfloat = pyarrow.map_(pyarrow.uint16(), pyarrow.float16())
mapdatebool = pyarrow.map_(pyarrow.date64(), pyarrow.bool_())
mapboollistint = pyarrow.map_(pyarrow.bool_(), pyarrow.list_(pyarrow.list_(pyarrow.uint16())))
mapfloatmapintlistbool = pyarrow.map_(
    pyarrow.float64(), pyarrow.map_(pyarrow.int16(), pyarrow.list_(pyarrow.bool_()))
)

schema1 = pyarrow.schema(
    [
        ("a", smallint),
        ("b", bigint, False),
        ("c", smalldate, True),
    ]
)
schema2 = pyarrow.schema(
    [pyarrow.field("x", smalldate, nullable=True), pyarrow.field("y", listbool, nullable=False)]
)
schema3 = pyarrow.schema(
    [
        ("adsf", listlistbigdate, False),
        pyarrow.field("ghjk", mapintfloat, nullable=True),
    ]
)
schema4 = pyarrow.schema(
    [
        pyarrow.field("foo", mapfloatmapintlistbool),
        pyarrow.field("bar", mapboollistint, nullable=False),
    ]
)

struct1 = pyarrow.struct(list(schema1))
struct2 = pyarrow.struct(list(schema2))
struct3 = pyarrow.struct(list(schema3))
struct4 = pyarrow.struct(list(schema4))


@pytest.mark.parametrize(
    "type_",
    [
        smallint,
        bigint,
        string,
        smalldate,
        medfloat,
        bool_,
        listmedint,
        listbool,
        listlistbigdate,
        mapintfloat,
        mapdatebool,
        mapboollistint,
        mapfloatmapintlistbool,
        struct1,
        struct2,
        struct3,
        struct4,
    ],
)
def test_pyarrow_type_literal(type_: pyarrow.DataType):
    _test_pyarrow_literal(pyarrow_type_literal, type_)


@pytest.mark.parametrize(
    "type_",
    [
        schema1,
        schema2,
        schema3,
        schema4,
    ],
)
def test_pyarrow_schema_literal(type_: pyarrow.Schema):
    _test_pyarrow_literal(pyarrow_schema_literal, type_)


def _test_pyarrow_literal(render: Callable[[PyArrow], str], t: PyArrow):
    s = render(t)
    t_ = eval(s)
    assert t == t_
