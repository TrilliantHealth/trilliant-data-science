import math
import sqlite3
from dataclasses import dataclass
from unittest import mock

import pytest
from returns.curry import partial

from thds.core.sqlite import StructTable, connect, read, write_mappings
from thds.core.sqlite.structured import BadPrimaryKey, TableMeta, UnknownColumns, autometa_factory


@dataclass
class TstItem:
    # don't call it a TestItem - pytest gets confused.
    id: int
    name: str


@pytest.fixture
def _base_test_db_rows():
    return (
        dict(id=1, name="one"),
        dict(id=2, name="two"),
        dict(id=3, name="three"),
        dict(id=4, name="four"),
        dict(id=5, name="five"),
        dict(id=6, name="six"),
        dict(id=7, name="seven"),
        dict(id=8, name="eight"),
        dict(id=9, name="nine"),
        dict(id=10, name="ten"),
        dict(id=11, name="more than ten"),
        dict(id=12, name="more than ten"),
        dict(id=13, name="more than ten"),
        dict(id=14, name="more than ten"),
        dict(id=15, name="more than ten"),
    )


@pytest.fixture
def _base_test_db(_base_test_db_rows) -> sqlite3.Connection:
    db = connect.row_connect(":memory:")
    db.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
    write_mappings(
        db,
        "test",
        _base_test_db_rows,
        batch_size=5,  # just to increase coverage a bit
    )
    db.commit()
    return db


@pytest.fixture
def test_db(_base_test_db) -> StructTable[TstItem]:
    return StructTable(
        lambda d: TstItem(**d),
        lambda _: lambda: TableMeta(_base_test_db, "test", {"id"}, {"id", "name"}),  # type: ignore
    )


def test_struct_table_basics(test_db: StructTable[TstItem]):
    assert test_db.get(id=1) == TstItem(1, "one")
    assert test_db.get(id=2) == TstItem(2, "two")
    assert not test_db.get(id=100)

    assert len(test_db.list(name="more than ten")) == 5

    with pytest.raises(ValueError):
        test_db.get(name="five")


def test_unknown_columns(test_db: StructTable[TstItem]):
    with pytest.raises(UnknownColumns):
        test_db.list(id=2, foo="bad")


def test_bad_primary_key(_base_test_db: sqlite3.Connection):
    bad_db = StructTable(
        lambda d: TstItem(**d),
        # name is not actually the primary key, so this (can) blow up
        lambda _: lambda: TableMeta(_base_test_db, "test", {"name"}, {"id", "name"}),  # type: ignore
    )
    assert bad_db.get(name="seven") == TstItem(7, "seven")

    with pytest.raises(BadPrimaryKey):
        bad_db.get(id=7)

    with pytest.raises(BadPrimaryKey):
        bad_db.get(name="more than ten")


def test_autometa(test_db: StructTable[TstItem]):
    with mock.patch("thds.core.sqlite.structured.row_connect", lambda _path: test_db._tbl().conn):
        table = StructTable(lambda d: TstItem(**d), autometa_factory(lambda: (":memory:", "test")))
        assert table.get(id=1) == TstItem(1, "one")


@pytest.mark.parametrize(
    "columns",
    [
        (None),
        ("name"),
        (["id", "name"]),
    ],
)
def test_partition_read(columns, _base_test_db: sqlite3.Connection, _base_test_db_rows):
    n = 4
    _partition = partial(read.partition, n, columns=columns)
    partitions = list(map(_partition, range(n)))
    _read_matching = partial(read.matching, "test", _base_test_db)
    results = list(map(list, map(_read_matching, partitions)))
    row_count = len(_base_test_db_rows)
    returned_row_count = sum(len(result) for result in results)

    assert row_count == returned_row_count

    if columns is None:
        for result in results:
            partition_size = len(result)
            assert (
                math.floor(row_count / n) <= partition_size <= math.ceil(row_count / n)
            ), "Expected evenly divided partitions when not partitioning on columns"
