"""Tests for the `wt status` command."""

from pathlib import Path

from tests.conftest import git_add, git_commit, git_run, write_file


def _worktree(repo: Path, run_wt, branch: str) -> Path:
    run_wt("co", [branch], cwd=repo / "main")
    return repo / branch


def test_clean_worktree_says_nothing(worktree_git_repo, run_wt):
    result = run_wt("status", [], cwd=worktree_git_repo / "main")

    assert result.returncode == 0
    assert result.stdout.strip() == ""


def test_reports_uncommitted_changes(worktree_git_repo, run_wt):
    write_file(worktree_git_repo / "main", "dirty.txt", "uncommitted\n")

    result = run_wt("status", [], cwd=worktree_git_repo / "main")

    assert "1 dirty" in result.stdout


def test_reports_from_a_subdirectory(worktree_git_repo, run_wt):
    write_file(worktree_git_repo / "main", "nested/deep/file.txt", "uncommitted\n")

    result = run_wt("status", [], cwd=worktree_git_repo / "main" / "nested" / "deep")

    assert "1 dirty" in result.stdout


def test_reports_directory_branch_mismatch(worktree_git_repo, run_wt):
    """The convention gent relies on is that a worktree's directory name IS its branch."""
    worktree = _worktree(worktree_git_repo, run_wt, "feature/original")
    git_run(worktree, "switch", "-c", "feature/switched-underneath")

    result = run_wt("status", ["feature/original"], cwd=worktree_git_repo / "main")

    assert "dir!=HEAD" in result.stdout
    assert "feature/original" in result.stdout
    assert "feature/switched-underneath" in result.stdout


def test_finds_worktree_by_branch_name_after_mismatch(worktree_git_repo, run_wt):
    worktree = _worktree(worktree_git_repo, run_wt, "feature/original")
    git_run(worktree, "switch", "-c", "feature/switched-underneath")

    result = run_wt("status", ["feature/switched-underneath"], cwd=worktree_git_repo / "main")

    assert "dir!=HEAD" in result.stdout


def test_reports_commits_not_in_base(worktree_git_repo, run_wt):
    worktree = _worktree(worktree_git_repo, run_wt, "feature/ahead")
    write_file(worktree, "new.txt", "content\n")
    git_add(worktree, ".")
    git_commit(worktree, "a commit not on main")

    result = run_wt("status", ["feature/ahead", "--base", "main"], cwd=worktree_git_repo / "main")

    assert "1 not in main" in result.stdout


def test_all_covers_every_worktree_and_stays_silent_about_clean_ones(worktree_git_repo, run_wt):
    _worktree(worktree_git_repo, run_wt, "feature/quiet")
    noisy = _worktree(worktree_git_repo, run_wt, "feature/noisy")
    write_file(noisy, "dirty.txt", "uncommitted\n")

    result = run_wt("status", ["--all"], cwd=worktree_git_repo / "main")

    assert "feature/noisy: 1 dirty" in result.stdout
    assert "feature/quiet" not in result.stdout


def test_all_emits_one_line_per_worktree(worktree_git_repo, run_wt):
    """Callers parse this output, so a long status must not wrap."""
    worktree = _worktree(worktree_git_repo, run_wt, "feature/a-rather-long-branch-name-for-wrapping")
    write_file(worktree, "dirty.txt", "uncommitted\n")
    git_run(worktree, "switch", "-c", "feature/an-even-longer-branch-name-to-force-a-wrap")

    result = run_wt("status", ["--all"], cwd=worktree_git_repo / "main")

    assert len([line for line in result.stdout.splitlines() if line.strip()]) == 1


def test_bare_repo_is_not_reported(worktree_git_repo, run_wt):
    result = run_wt("status", ["--all"], cwd=worktree_git_repo / "main")

    assert ".bare" not in result.stdout


def test_unknown_target_warns(worktree_git_repo, run_wt):
    result = run_wt("status", ["feature/does-not-exist"], cwd=worktree_git_repo / "main")

    assert result.stdout.strip() == ""
    assert "No worktree found" in result.stderr
