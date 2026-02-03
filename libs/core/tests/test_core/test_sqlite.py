import math
import sqlite3
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from unittest import mock

import pytest

from thds.core.sqlite import StructTable, connect, read, write_mappings
from thds.core.sqlite.meta import get_table_schema
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


def test_function_registration(_base_test_db: sqlite3.Connection):
    def _is_func_registered(conn: sqlite3.Connection, func_name: str) -> bool:
        try:
            # Attempt to use the function in a simple query
            conn.execute(f"SELECT {func_name}('test')")
            return True
        except sqlite3.OperationalError:
            return False

    assert _is_func_registered(_base_test_db, "_pyhash_values"), (
        "Function _pyhash_values is not registered"
    )


@pytest.mark.parametrize(
    "columns",
    [
        None,
        "name",
        (["id", "name"]),
    ],
)
def test_partition_read(columns, _base_test_db: sqlite3.Connection, _base_test_db_rows):
    n = 4
    _partition = partial(read.partition, n, columns=columns)
    partitions = list(map(_partition, range(n)))
    _read_matching = partial(read.matching, "test", _base_test_db)
    results = list(map(list, map(_read_matching, partitions)))  # type: ignore
    row_count = len(_base_test_db_rows)
    returned_row_count = sum(len(result) for result in results)

    assert row_count == returned_row_count

    if columns is None:
        for result in results:
            partition_size = len(result)
            assert math.floor(row_count / n) <= partition_size <= math.ceil(row_count / n), (
                "Expected evenly divided partitions when not partitioning on columns"
            )


def test_get_table_schema_with_sqlite_connection(_base_test_db: sqlite3.Connection):
    schema = get_table_schema(_base_test_db, "test")
    expected_schema = {"id": "integer", "name": "text"}
    assert schema == expected_schema


def test_get_table_schema_with_connectable(_base_test_db: sqlite3.Connection, tmp_path: Path):
    # Create a temporary SQLite database file
    db_path = tmp_path / "test.db"

    # Write the in-memory database to the temporary file
    with sqlite3.connect(db_path) as conn:
        # Iterate over the SQL statements that would recreate the in-memory database
        for line in _base_test_db.iterdump():
            # Filter out lines related to internal FTS5 tables to avoid operational errors
            if all(
                internal_table not in line
                for internal_table in [
                    "test_table_fts_config",
                    "test_table_fts_data",
                    "test_table_fts_content",
                    "test_table_fts_idx",
                    "test_table_fts_docsize",
                    "test_table_fts_parent",
                ]
            ):
                conn.execute(line)

    # Pass the path to the function
    schema = get_table_schema(db_path, "test")
    expected_schema = {"id": "integer", "name": "text"}
    assert schema == expected_schema


def test_format_bad_data():
    import textwrap

    from thds.core.sqlite.write import _format_bad_data

    assert (
        _format_bad_data(("foobar", 3, 1.2))
        == textwrap.dedent(
            """
    COL 1: foobar
           <class 'str'>
    COL 2: 3
           <class 'int'>
    COL 3: 1.2
           <class 'float'>
    """
        ).strip()
    )

    assert (
        _format_bad_data(dict(a="foobar", b=3, c=1.2))
        == textwrap.dedent(
            """
    COL a: foobar
           <class 'str'>
    COL b: 3
           <class 'int'>
    COL c: 1.2
           <class 'float'>
    """
        ).strip()
    )
