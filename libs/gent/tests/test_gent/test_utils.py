"""
Tests for utility functions, especially error detection.
"""

import os
from pathlib import Path

import pytest

from thds.gent._repo import GENT_REPO_PATH
from thds.gent.utils import find_worktree_root, get_worktree_root_or_exit


def test_find_worktree_root_in_worktree(worktree_git_repo):
    """Test finding worktree root from within a worktree."""
    main_path = worktree_git_repo / "main"

    # From the main worktree
    root = find_worktree_root(main_path)
    assert root == worktree_git_repo.resolve()

    # From a subdirectory within the worktree
    subdir = main_path / "subdir"
    subdir.mkdir()
    root = find_worktree_root(subdir)
    assert root == worktree_git_repo.resolve()


def test_find_worktree_root_at_root(worktree_git_repo):
    """Test finding worktree root when already at the root."""
    root = find_worktree_root(worktree_git_repo)
    assert root == worktree_git_repo.resolve()


def test_find_worktree_root_in_regular_repo(regular_git_repo):
    """Test finding worktree root in a regular (non-bare) repository returns None."""
    root = find_worktree_root(regular_git_repo)
    assert root is None


def test_find_worktree_root_in_non_git_directory(tmp_path):
    """Test finding worktree root in a non-git directory returns None."""
    non_git_dir = tmp_path / "not-a-repo"
    non_git_dir.mkdir()

    root = find_worktree_root(non_git_dir)
    assert root is None


def test_get_worktree_root_or_exit_success(worktree_git_repo):
    """Test get_worktree_root_or_exit succeeds in a worktree."""
    main_path = worktree_git_repo / "main"

    # Change to the main worktree directory
    original_cwd = Path.cwd()
    try:
        os.chdir(main_path)
        root = get_worktree_root_or_exit()
        assert root == worktree_git_repo.resolve()
    finally:
        os.chdir(original_cwd)


def test_get_worktree_root_or_exit_regular_repo(regular_git_repo):
    """Test get_worktree_root_or_exit exits with helpful message in regular repo."""
    original_cwd = Path.cwd()
    try:
        os.chdir(regular_git_repo)

        with pytest.raises(SystemExit) as exc_info:
            get_worktree_root_or_exit()

        assert exc_info.value.code == 1

        # The error message should be captured by capsys if we were capturing it,
        # but since we're using error_exit which writes to stderr and calls sys.exit,
        # we can at least verify it exits with code 1
    finally:
        os.chdir(original_cwd)


def test_get_worktree_root_or_exit_regular_repo_message(regular_git_repo, capsys):
    """Test get_worktree_root_or_exit shows helpful message for regular repo."""
    original_cwd = Path.cwd()
    try:
        os.chdir(regular_git_repo)

        with pytest.raises(SystemExit) as exc_info:
            get_worktree_root_or_exit()

        # Check the error message in stderr
        captured = capsys.readouterr()
        assert "regular git repository" in captured.err
        assert "wt requires a bare repository structure" in captured.err
        # Check URL is present (may be wrapped with line breaks by Rich)
        assert "github.com" in captured.err
        assert GENT_REPO_PATH in captured.err
        assert "Quick setup" in captured.err or "wt clone" in captured.err
        # Should NOT have detailed conversion steps anymore
        assert "Quick conversion:" not in captured.err

        assert exc_info.value.code == 1
    finally:
        os.chdir(original_cwd)


def test_get_worktree_root_or_exit_non_git_directory(tmp_path, capsys):
    """Test get_worktree_root_or_exit exits with clear message in non-git directory."""
    non_git_dir = tmp_path / "not-a-repo"
    non_git_dir.mkdir()

    original_cwd = Path.cwd()
    try:
        os.chdir(non_git_dir)

        with pytest.raises(SystemExit) as exc_info:
            get_worktree_root_or_exit()

        # Check the error message in stderr
        captured = capsys.readouterr()
        assert "Not in a git repository" in captured.err
        assert "wt requires a bare repository structure" in captured.err
        assert "To get started" in captured.err or "wt clone" in captured.err
        # Check URL is present (may be wrapped with line breaks by Rich)
        assert "github.com" in captured.err
        assert GENT_REPO_PATH in captured.err
        # Should NOT have the detailed regular repo message
        assert "regular git repository" not in captured.err

        assert exc_info.value.code == 1
    finally:
        os.chdir(original_cwd)


def test_get_worktree_root_or_exit_from_subdirectory(worktree_git_repo):
    """Test get_worktree_root_or_exit works from deep subdirectory."""
    main_path = worktree_git_repo / "main"
    deep_subdir = main_path / "src" / "components" / "ui"
    deep_subdir.mkdir(parents=True)

    original_cwd = Path.cwd()
    try:
        os.chdir(deep_subdir)
        root = get_worktree_root_or_exit()
        assert root == worktree_git_repo.resolve()
    finally:
        os.chdir(original_cwd)
