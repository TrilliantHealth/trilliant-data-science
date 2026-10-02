"""Tests for `wt rm --trash`."""

import os
import subprocess
import sys
import time
from pathlib import Path

from thds.gent import worktree_trash

from tests.conftest import git_run, write_file


def _wait_until_empty(trash: Path, timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while any(trash.iterdir()):
        assert time.monotonic() < deadline, f"trash not emptied: {sorted(trash.iterdir())}"
        time.sleep(0.1)


def test_rm_trash_forgets_worktree_and_empties_trash(worktree_git_repo, run_wt):
    bare_path = worktree_git_repo / ".bare"
    run_wt("co", ["feature/gone"], cwd=worktree_git_repo / "main")

    result = run_wt("rm", ["feature/gone", "--trash"], cwd=worktree_git_repo / "main")

    assert result.returncode == 0, result.stderr
    assert not (worktree_git_repo / "feature").exists()
    assert "feature/gone" not in git_run(bare_path, "worktree", "list").stdout
    assert "feature/gone" not in git_run(bare_path, "branch", "--list", "feature/gone").stdout
    _wait_until_empty(worktree_trash.trash_dir(bare_path))


def test_rm_trash_refuses_dirty_worktree_without_force(worktree_git_repo, run_wt):
    run_wt("co", ["feature/dirty"], cwd=worktree_git_repo / "main")
    worktree_path = worktree_git_repo / "feature" / "dirty"
    write_file(worktree_path, "untracked.txt", "uncommitted")

    result = run_wt("rm", ["feature/dirty", "--trash"], cwd=worktree_git_repo / "main")

    assert result.returncode != 0
    assert "wt rm feature/dirty --trash -f" in result.stderr
    assert (worktree_path / "untracked.txt").exists()

    result = run_wt("rm", ["feature/dirty", "--trash", "-f"], cwd=worktree_git_repo / "main")

    assert result.returncode == 0, result.stderr
    assert not worktree_path.exists()


def test_rm_trash_refuses_unmerged_branch_without_force(worktree_git_repo, run_wt):
    run_wt("co", ["feature/unmerged"], cwd=worktree_git_repo / "main")
    worktree_path = worktree_git_repo / "feature" / "unmerged"
    write_file(worktree_path, "new.txt", "content")
    git_run(worktree_path, "add", ".")
    git_run(worktree_path, "commit", "-m", "unmerged")

    result = run_wt("rm", ["feature/unmerged", "--trash"], cwd=worktree_git_repo / "main")

    assert result.returncode != 0
    assert worktree_path.exists()


def test_rm_trash_refuses_locked_worktree(worktree_git_repo, run_wt):
    bare_path = worktree_git_repo / ".bare"
    run_wt("co", ["feature/locked"], cwd=worktree_git_repo / "main")
    worktree_path = worktree_git_repo / "feature" / "locked"
    git_run(bare_path, "worktree", "lock", str(worktree_path))

    result = run_wt("rm", ["feature/locked", "--trash", "-f"], cwd=worktree_git_repo / "main")

    assert result.returncode != 0
    assert "locked" in result.stderr
    assert worktree_path.exists()


def test_rm_trash_sweeps_leftovers(worktree_git_repo, run_wt):
    bare_path = worktree_git_repo / ".bare"
    trash = worktree_trash.trash_dir(bare_path)
    write_file(trash / "left-behind-1234abcd", "some/file.txt", "stale")
    run_wt("co", ["feature/next"], cwd=worktree_git_repo / "main")

    result = run_wt("rm", ["feature/next", "--trash"], cwd=worktree_git_repo / "main")

    assert result.returncode == 0, result.stderr
    _wait_until_empty(trash)


def test_rm_trash_run_by_python_inside_the_worktree(worktree_git_repo, run_wt):
    run_wt("co", ["feature/self"], cwd=worktree_git_repo / "main")
    python = worktree_git_repo / "feature" / "self" / "bin" / "python"
    python.parent.mkdir()
    python.symlink_to(sys.executable)
    env = {**os.environ, "PYTHONPATH": os.pathsep.join(sys.path)}

    result = subprocess.run(
        [str(python), "-m", "thds.gent", "rm", "feature/self", "--trash", "-f"],
        cwd=worktree_git_repo / "main",
        env=env,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    _wait_until_empty(worktree_trash.trash_dir(worktree_git_repo / ".bare"))
