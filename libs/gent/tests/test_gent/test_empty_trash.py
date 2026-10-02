"""Tests for the background trash deleter."""

import os

from thds.gent import empty_trash

from tests.conftest import write_file


def test_empty_leaves_work_to_the_lock_holder(tmp_path):
    trash = tmp_path / "gent-trash"
    write_file(trash / "wt-1", "a.txt", "a")

    fd = empty_trash._try_lock(trash)
    assert fd is not None
    empty_trash.empty(trash)
    assert (trash / "wt-1").exists()

    os.close(fd)
    empty_trash.empty(trash)
    assert not any(trash.iterdir())
