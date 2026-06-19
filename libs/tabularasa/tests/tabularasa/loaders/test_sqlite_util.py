from pathlib import Path

import pytest

from thds.tabularasa.loaders.sqlite_util import bulk_write_connection


def _lock_path(db_path: Path) -> Path:
    return db_path.with_name(db_path.name + ".lock")


def test_bulk_write_connection_keeps_lock_file(tmp_path: Path) -> None:
    """The lock file must outlive the context manager.

    Deleting it on exit raced with `filelock._acquire`, which only sets O_CREAT when the file is
    absent: a concurrent worker deleting the file between another's existence check and its os.open
    raised FileNotFoundError. Keeping the (0-byte) file on disk is how `filelock` itself avoids this.
    """
    db_path = tmp_path / "regression.db"

    with bulk_write_connection(db_path) as con:
        con.execute("CREATE TABLE t (x INTEGER)")

    assert _lock_path(db_path).exists(), "lock file was deleted; this re-introduces the TOCTOU race"


def test_bulk_write_connection_reacquires_existing_lock(tmp_path: Path) -> None:
    """Acquiring the lock a second time must work even though the file already exists on disk -
    `filelock` reuses it rather than treating its presence as 'already locked'."""
    db_path = tmp_path / "reacquire.db"

    with bulk_write_connection(db_path) as con:
        con.execute("CREATE TABLE t (x INTEGER)")

    # The lock file now exists; a subsequent acquisition against the same path must still succeed.
    with bulk_write_connection(db_path) as con:
        con.execute("INSERT INTO t (x) VALUES (1)")
        con.commit()  # commit before exit; the context resets PRAGMA synchronous, which sqlite forbids mid-transaction

    # SELECT opens no transaction, so the default close=True path resets the PRAGMA and closes cleanly.
    with bulk_write_connection(db_path) as con:
        assert con.execute("SELECT count(*) FROM t").fetchone()[0] == 1


@pytest.mark.parametrize("db_name", ["suffix.db", "suffix.sqlite"])
def test_bulk_write_connection_lock_mirrors_db_filename(tmp_path: Path, db_name: str) -> None:
    """The lock sits beside the db as `<db filename>.lock`, so a `.db` db gets `.db.lock` and a
    `.sqlite` db gets `.sqlite.lock`. Both suffixes have matching gitignore rules, keeping the lock
    out of version control the same way `*.db` / `*.sqlite` already cover the database file itself."""
    db_path = tmp_path / db_name

    with bulk_write_connection(db_path) as con:
        con.execute("CREATE TABLE t (x INTEGER)")

    assert (tmp_path / f"{db_name}.lock").exists()
    assert not (tmp_path / "suffix.lock").exists()  # the full filename is kept, not just the stem
