"""Tests for the `wt prune` command."""

import os
import time
from pathlib import Path

from tests.conftest import git_run, write_file


def _backdate(path: Path, days: int = 4) -> None:
    """Set directory mtime to the past so it passes the 3-day age threshold."""
    old_time = time.time() - days * 86400
    os.utime(path, (old_time, old_time))


def _worktree(repo: Path, run_wt, branch: str) -> Path:
    run_wt("co", [branch], cwd=repo / "main")
    path = repo / branch
    _backdate(path)
    return path


def _add_remote_ref(bare_path: Path, branch: str) -> None:
    """Simulate a remote branch by creating refs/remotes/origin/<branch>."""
    result = git_run(bare_path, "rev-parse", branch)
    sha = result.stdout.strip()
    git_run(bare_path, "update-ref", f"refs/remotes/origin/{branch}", sha)


def _add_local_commit(worktree_path: Path) -> None:
    """Add a commit not in main, making the worktree 'unmerged'."""
    write_file(worktree_path, "local-change.txt", "content\n")
    git_run(worktree_path, "add", ".")
    git_run(worktree_path, "commit", "-m", "local commit")


def test_nothing_to_prune_when_all_have_remotes(worktree_git_repo, run_wt):
    bare_path = worktree_git_repo / ".bare"
    _add_remote_ref(bare_path, "main")
    feature = _worktree(worktree_git_repo, run_wt, "feature/kept")
    _add_remote_ref(bare_path, "feature/kept")

    result = run_wt("prune", ["--yes"], cwd=worktree_git_repo / "main")

    assert result.returncode == 0
    assert "Nothing to prune" in result.stdout
    assert feature.exists()


def test_prunes_worktree_without_remote_branch(worktree_git_repo, run_wt):
    bare_path = worktree_git_repo / ".bare"
    _add_remote_ref(bare_path, "main")

    gone = _worktree(worktree_git_repo, run_wt, "feature/gone")
    assert gone.exists()

    result = run_wt("prune", ["--yes"], cwd=worktree_git_repo / "main")

    assert result.returncode == 0
    assert not gone.exists()


def test_keeps_worktree_with_remote_branch(worktree_git_repo, run_wt):
    bare_path = worktree_git_repo / ".bare"
    _add_remote_ref(bare_path, "main")

    kept = _worktree(worktree_git_repo, run_wt, "feature/kept")
    _add_remote_ref(bare_path, "feature/kept")

    gone = _worktree(worktree_git_repo, run_wt, "feature/gone")

    result = run_wt("prune", ["--yes"], cwd=worktree_git_repo / "main")

    assert result.returncode == 0
    assert kept.exists(), "worktree with remote branch should be kept"
    assert not gone.exists(), "worktree without remote branch should be removed"


def test_yes_skips_dirty_worktrees(worktree_git_repo, run_wt):
    """--yes without --force skips dirty worktrees."""
    bare_path = worktree_git_repo / ".bare"
    _add_remote_ref(bare_path, "main")

    dirty = _worktree(worktree_git_repo, run_wt, "feature/dirty")
    write_file(dirty, "uncommitted.txt", "dirty content\n")
    _backdate(dirty)

    result = run_wt("prune", ["--yes"], cwd=worktree_git_repo / "main")

    assert result.returncode == 0
    assert dirty.exists(), "dirty worktree should be skipped without --force"
    combined = result.stdout + result.stderr
    assert "skipped" in combined.lower()


def test_yes_force_removes_dirty_worktrees(worktree_git_repo, run_wt):
    """--yes --force removes dirty worktrees."""
    bare_path = worktree_git_repo / ".bare"
    _add_remote_ref(bare_path, "main")

    dirty = _worktree(worktree_git_repo, run_wt, "feature/dirty")
    write_file(dirty, "uncommitted.txt", "dirty content\n")
    _backdate(dirty)

    result = run_wt("prune", ["--yes", "--force"], cwd=worktree_git_repo / "main")

    assert result.returncode == 0
    assert not dirty.exists(), "dirty worktree should be removed with --force"


def test_yes_skips_unmerged_worktrees(worktree_git_repo, run_wt):
    """--yes without --force skips worktrees with commits not in main."""
    bare_path = worktree_git_repo / ".bare"
    _add_remote_ref(bare_path, "main")

    unmerged = _worktree(worktree_git_repo, run_wt, "feature/unmerged")
    _add_local_commit(unmerged)
    _backdate(unmerged)

    result = run_wt("prune", ["--yes"], cwd=worktree_git_repo / "main")

    assert result.returncode == 0
    assert unmerged.exists(), "unmerged worktree should be skipped without --force"
    combined = result.stdout + result.stderr
    assert "skipped" in combined.lower()


def test_yes_force_removes_unmerged_worktrees(worktree_git_repo, run_wt):
    """--yes --force removes worktrees with commits not in main."""
    bare_path = worktree_git_repo / ".bare"
    _add_remote_ref(bare_path, "main")

    unmerged = _worktree(worktree_git_repo, run_wt, "feature/unmerged")
    _add_local_commit(unmerged)
    _backdate(unmerged)

    result = run_wt("prune", ["--yes", "--force"], cwd=worktree_git_repo / "main")

    assert result.returncode == 0
    assert not unmerged.exists(), "unmerged worktree should be removed with --force"


def test_cleans_empty_parent_directories(worktree_git_repo, run_wt):
    bare_path = worktree_git_repo / ".bare"
    _add_remote_ref(bare_path, "main")

    nested = _worktree(worktree_git_repo, run_wt, "release/old")
    assert nested.exists()
    assert (worktree_git_repo / "release").exists()

    result = run_wt("prune", ["--yes"], cwd=worktree_git_repo / "main")

    assert result.returncode == 0
    assert not nested.exists()
    assert not (worktree_git_repo / "release").exists(), "empty parent dir should be removed"


def test_deletes_local_branch(worktree_git_repo, run_wt):
    bare_path = worktree_git_repo / ".bare"
    _add_remote_ref(bare_path, "main")

    _worktree(worktree_git_repo, run_wt, "feature/gone")

    result = run_wt("prune", ["--yes"], cwd=worktree_git_repo / "main")

    assert result.returncode == 0

    branches = git_run(bare_path, "branch", "--list", "feature/gone")
    assert "feature/gone" not in branches.stdout


def test_prunes_multiple_worktrees(worktree_git_repo, run_wt):
    bare_path = worktree_git_repo / ".bare"
    _add_remote_ref(bare_path, "main")

    a = _worktree(worktree_git_repo, run_wt, "feature/a")
    b = _worktree(worktree_git_repo, run_wt, "feature/b")
    kept = _worktree(worktree_git_repo, run_wt, "feature/kept")
    _add_remote_ref(bare_path, "feature/kept")

    result = run_wt("prune", ["--yes"], cwd=worktree_git_repo / "main")

    assert result.returncode == 0
    assert not a.exists()
    assert not b.exists()
    assert kept.exists()


def test_skips_worktrees_with_missing_directory(worktree_git_repo, run_wt):
    """A worktree whose directory was deleted needs git worktree prune, not wt prune."""
    import shutil

    bare_path = worktree_git_repo / ".bare"
    _add_remote_ref(bare_path, "main")

    deleted = _worktree(worktree_git_repo, run_wt, "feature/deleted")
    shutil.rmtree(deleted)

    result = run_wt("prune", ["--yes"], cwd=worktree_git_repo / "main")

    assert result.returncode == 0


def test_aborts_when_no_remote_configured(worktree_git_repo, run_wt):
    """When no remote is configured, prune aborts (remote view untrusted)."""
    result = run_wt("prune", ["--yes"], cwd=worktree_git_repo / "main")

    assert result.returncode != 0
    combined = result.stdout + result.stderr
    assert "remote" in combined.lower()


def test_aborts_when_default_branch_missing_from_remote(worktree_git_repo, run_wt):
    """When the default branch is not in remote refs, abort."""
    bare_path = worktree_git_repo / ".bare"
    # Add a remote ref for a feature branch but NOT for main
    feature = _worktree(worktree_git_repo, run_wt, "feature/only")
    _add_remote_ref(bare_path, "feature/only")

    result = run_wt("prune", ["--yes"], cwd=worktree_git_repo / "main")

    assert result.returncode != 0
    assert feature.exists(), "nothing should be removed when remote view is untrusted"


def test_selective_removal_by_number(worktree_git_repo, run_wt):
    """Typing a single number removes only that worktree."""
    bare_path = worktree_git_repo / ".bare"
    _add_remote_ref(bare_path, "main")

    a = _worktree(worktree_git_repo, run_wt, "feature/a")
    b = _worktree(worktree_git_repo, run_wt, "feature/b")

    result = run_wt("prune", [], cwd=worktree_git_repo / "main", input_text="1\n")

    assert result.returncode == 0
    assert not a.exists(), "selected worktree should be removed"
    assert b.exists(), "unselected worktree should be kept"


def test_select_all_removes_everything(worktree_git_repo, run_wt):
    """Typing 'all' removes every candidate."""
    bare_path = worktree_git_repo / ".bare"
    _add_remote_ref(bare_path, "main")

    a = _worktree(worktree_git_repo, run_wt, "feature/a")
    b = _worktree(worktree_git_repo, run_wt, "feature/b")

    result = run_wt("prune", [], cwd=worktree_git_repo / "main", input_text="all\n")

    assert result.returncode == 0
    assert not a.exists()
    assert not b.exists()


def test_select_none_aborts(worktree_git_repo, run_wt):
    """Empty input aborts without removing anything."""
    bare_path = worktree_git_repo / ".bare"
    _add_remote_ref(bare_path, "main")

    a = _worktree(worktree_git_repo, run_wt, "feature/a")

    result = run_wt("prune", [], cwd=worktree_git_repo / "main", input_text="\n")

    assert result.returncode == 0
    assert a.exists(), "empty input should abort without removing anything"


def test_interactive_blocks_unsafe_without_force(worktree_git_repo, run_wt):
    """Selecting an unsafe candidate without --force warns and aborts."""
    bare_path = worktree_git_repo / ".bare"
    _add_remote_ref(bare_path, "main")

    unmerged = _worktree(worktree_git_repo, run_wt, "feature/unmerged")
    _add_local_commit(unmerged)
    _backdate(unmerged)

    result = run_wt("prune", [], cwd=worktree_git_repo / "main", input_text="1\n")

    assert result.returncode == 0
    assert unmerged.exists(), "unsafe worktree should not be removed without --force"
    combined = result.stdout + result.stderr
    assert "force" in combined.lower()


def test_interactive_force_allows_unsafe(worktree_git_repo, run_wt):
    """With --force, unsafe candidates can be selected interactively."""
    bare_path = worktree_git_repo / ".bare"
    _add_remote_ref(bare_path, "main")

    unmerged = _worktree(worktree_git_repo, run_wt, "feature/unmerged")
    _add_local_commit(unmerged)
    _backdate(unmerged)

    result = run_wt("prune", ["--force"], cwd=worktree_git_repo / "main", input_text="1\n")

    assert result.returncode == 0
    assert not unmerged.exists(), "unsafe worktree should be removed with --force"


def test_skips_fresh_worktrees(worktree_git_repo, run_wt):
    """Worktrees created less than 3 days ago are not candidates."""
    bare_path = worktree_git_repo / ".bare"
    _add_remote_ref(bare_path, "main")

    # _worktree backdates by default; create one without backdating
    run_wt("co", ["feature/fresh"], cwd=worktree_git_repo / "main")
    fresh = worktree_git_repo / "feature/fresh"
    # Do NOT backdate — it should be skipped as too new

    result = run_wt("prune", ["--yes"], cwd=worktree_git_repo / "main")

    assert result.returncode == 0
    assert fresh.exists(), "fresh worktree should be skipped (< 3 days old)"
