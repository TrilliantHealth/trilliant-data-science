import sqlite3

import pytest

from thds.core.sqlite import connect, read, upsert, write


@pytest.fixture
def _base_test_db_rows():
    return (
        dict(id=1, name="one", foo="bar"),
        dict(id=2, name="two", foo="baz", age=19),
        dict(id=3, name="three", foo="qux", age=20),
        dict(id=4, name="four", foo="quux", age=21),
        dict(id=5, name="five", age=45, countdown=99),
        dict(id=6, name="six", age=46, countdown=98),
        dict(id=7, name="seven", age=47, countdown=97),
        dict(id=8, name="eight", height=1.8),
        dict(id=9, name="nine", height=1.9, countdown=22),
        dict(id=10, name="ten", height=2.0, countdown=21),
        dict(id=11, name="more than ten", allowed_bags=9),
        dict(id=12, name="more than ten", allowed_bags=10),
        dict(id=13, name="more than ten", allowed_bags=0, age=88, foo="whatever"),
        dict(id=14, name="more than ten"),
        dict(id=15, name="more than ten", untouched="TMNT"),
    )


TEST_TB = "test"


@pytest.fixture
def test_conn(_base_test_db_rows) -> sqlite3.Connection:
    db = connect.row_connect(":memory:")
    db.execute(
        "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT NOT NULL, foo TEXT, age INTEGER, height REAL, countdown INTEGER, allowed_bags INTEGER, untouched TEXT)"
    )
    for item in _base_test_db_rows:
        write.write_mappings(db, TEST_TB, [item])
    return db


def test_upsert_does_not_overwrite_missing_cols(test_conn: sqlite3.Connection):
    upsert.mappings(
        test_conn,
        TEST_TB,
        [
            dict(id=42, name="42", foo="42"),  # pure insert
            dict(id=1, name="one", foo="SPAZ"),  # existing/conflict
            # ^ these form a batch, and one is a conflict and the other a pure insert.
            dict(id=30, name="insert-me", age=10),
            dict(id=1, name="one", foo="SPEZ", age=10),
            # ^ can handle having multiple rows for the same key
            dict(id=8, age=8, countdown=13),
            dict(id=9, age=9, countdown=13),  # these two rows should form a batch
            dict(id=10, age=10, name="GEORGE", countdown=13),
            dict(id=1, age=1, countdown=13),
        ],
        batch_size=3,
    )
    assert dict(test_conn.execute("SELECT * FROM test WHERE id = 1").fetchone()) == dict(
        id=1, name="one", foo="SPEZ", age=1, countdown=13, height=None, allowed_bags=None, untouched=None
    )
    assert dict(test_conn.execute("SELECT * FROM test WHERE id = 42").fetchone()) == dict(
        id=42,
        name="42",
        foo="42",
        age=None,
        countdown=None,
        height=None,
        allowed_bags=None,
        untouched=None,
    )
    assert dict(test_conn.execute("SELECT * FROM test WHERE id = 30").fetchone()) == dict(
        id=30,
        name="insert-me",
        foo=None,
        age=10,
        countdown=None,
        height=None,
        allowed_bags=None,
        untouched=None,
    )


def test_upsert_warns_if_no_rows(test_conn: sqlite3.Connection, caplog):
    upsert.mappings(test_conn, TEST_TB, [])
    caplog.records[0].getMessage().startswith("No rows to upsert")


def test_name_cannot_be_null_but_can_be_omitted_from_update(test_conn: sqlite3.Connection):
    with pytest.raises(sqlite3.IntegrityError):
        upsert.mappings(test_conn, TEST_TB, [dict(id=99, age=88)])

    with pytest.raises(sqlite3.IntegrityError):
        upsert.mappings(test_conn, TEST_TB, [dict(id=99, name=None)])

    upsert.mappings(test_conn, TEST_TB, [dict(id=99, name="George Smythe")])
    # basic insert to get things moving
    upsert.mappings(test_conn, TEST_TB, [dict(id=99, age=88)])
    # would not previously have worked, but since the row exists, this will now be okay.

    assert dict(list(read.matching_select(TEST_TB, test_conn, dict(id=99)))[0]) == dict(
        id=99,
        name="George Smythe",
        age=88,
        allowed_bags=None,
        countdown=None,
        foo=None,
        height=None,
        untouched=None,
    )
