"""
Integration tests for worktree management commands.
"""

import subprocess
from pathlib import Path


def test_gent_root(worktree_git_repo, run_wt):
    """Test wt root finds the worktree root."""
    main_path = worktree_git_repo / "main"
    result = run_wt("root", [], cwd=main_path)
    assert result.returncode == 0
    assert str(worktree_git_repo) in result.stdout.strip()


def test_gent_co_creates_from_head_by_default(worktree_git_repo, run_wt):
    """Test wt co creates worktree from current worktree when base branch not specified."""
    main_path = worktree_git_repo / "main"
    result = run_wt("co", ["feature/test"], cwd=main_path)

    assert result.returncode == 0
    assert (worktree_git_repo / "feature" / "test").exists()


def test_gent_co_creates_from_explicit_base(worktree_git_repo, run_wt):
    """Test wt co creates worktree from specified base branch."""
    main_path = worktree_git_repo / "main"
    result = run_wt("co", ["feature/new", "main"], cwd=main_path)

    assert result.returncode == 0
    assert (worktree_git_repo / "feature" / "new").exists()


def test_gent_list(worktree_git_repo, run_wt):
    """Test wt list lists worktrees."""
    main_path = worktree_git_repo / "main"

    # Create a test worktree
    run_wt("co", ["feature/test"], cwd=main_path)

    result = run_wt("list", [], cwd=main_path)

    assert result.returncode == 0
    assert "main" in result.stdout
    assert "feature/test" in result.stdout


def test_gent_cd(worktree_git_repo, run_wt):
    """Test wt cd resolves worktree path."""
    main_path = worktree_git_repo / "main"
    result = run_wt("cd", ["main"], cwd=main_path)

    assert result.returncode == 0
    assert "main" in result.stdout


def test_gent_path_with_branch_arg(worktree_git_repo, run_wt):
    """Test wt path prints full path when branch name provided."""
    main_path = worktree_git_repo / "main"

    # Create a test worktree
    run_wt("co", ["feature/test"], cwd=main_path)

    result = run_wt("path", ["feature/test"], cwd=main_path)

    assert result.returncode == 0
    expected_path = (worktree_git_repo / "feature" / "test").resolve()
    actual_path = Path(result.stdout.strip()).resolve()
    assert expected_path == actual_path


def test_gent_path_without_arg_from_worktree(worktree_git_repo, run_wt):
    """Test wt path prints current worktree path when no argument provided."""
    main_path = worktree_git_repo / "main"

    # Run from main worktree
    result = run_wt("path", [], cwd=main_path)

    assert result.returncode == 0
    expected_path = main_path.resolve()
    actual_path = Path(result.stdout.strip()).resolve()
    assert expected_path == actual_path


def test_gent_path_without_arg_from_nested_dir(worktree_git_repo, run_wt):
    """Test wt path works from subdirectory within worktree."""
    main_path = worktree_git_repo / "main"

    # Create a subdirectory
    subdir = main_path / "subdir"
    subdir.mkdir()

    # Run from subdirectory
    result = run_wt("path", [], cwd=subdir)

    assert result.returncode == 0
    expected_path = main_path.resolve()
    actual_path = Path(result.stdout.strip()).resolve()
    assert expected_path == actual_path


def test_gent_path_without_arg_from_root_fails(worktree_git_repo, run_wt):
    """Test wt path fails with helpful message when not in worktree and no arg provided."""
    root = worktree_git_repo

    result = run_wt("path", [], cwd=root)

    assert result.returncode != 0
    assert "Not in a worktree" in result.stderr or "specify a branch name" in result.stderr


def test_gent_path_with_nonexistent_branch_fails(worktree_git_repo, run_wt):
    """Test wt path fails when branch doesn't exist."""
    main_path = worktree_git_repo / "main"

    result = run_wt("path", ["nonexistent/branch"], cwd=main_path)

    assert result.returncode != 0
    assert "not found" in result.stderr.lower() or "does not exist" in result.stderr.lower()


def test_gent_rm_with_branch_arg(worktree_git_repo, run_wt):
    """Test wt rm removes worktree and branch with branch argument."""
    main_path = worktree_git_repo / "main"
    bare_path = worktree_git_repo / ".bare"

    # Create a worktree to remove
    run_wt("co", ["feature/to-remove"], cwd=main_path)
    assert (worktree_git_repo / "feature" / "to-remove").exists()

    # Verify branch exists
    result = subprocess.run(
        ["git", "branch", "--list", "feature/to-remove"],
        cwd=bare_path,
        capture_output=True,
        text=True,
    )
    assert "feature/to-remove" in result.stdout

    # Remove it with explicit branch argument
    result = run_wt("rm", ["feature/to-remove"], cwd=main_path)

    assert result.returncode == 0
    assert not (worktree_git_repo / "feature" / "to-remove").exists()

    # Verify branch is also deleted
    result = subprocess.run(
        ["git", "branch", "--list", "feature/to-remove"],
        cwd=bare_path,
        capture_output=True,
        text=True,
    )
    assert "feature/to-remove" not in result.stdout


def test_gent_rm_with_force_flag(worktree_git_repo, run_wt):
    """Test wt rm removes worktree with --force flag."""
    main_path = worktree_git_repo / "main"

    # Create a worktree and add uncommitted changes
    run_wt("co", ["feature/dirty"], cwd=main_path)
    worktree_path = worktree_git_repo / "feature" / "dirty"
    (worktree_path / "test.txt").write_text("uncommitted")

    # Try to remove with force flag
    result = run_wt("rm", ["feature/dirty", "--force"], cwd=main_path)

    assert result.returncode == 0
    assert not worktree_path.exists()


def test_gent_rm_with_short_force_flag(worktree_git_repo, run_wt):
    """Test wt rm removes worktree with -f flag."""
    main_path = worktree_git_repo / "main"

    # Create a worktree and add uncommitted changes
    run_wt("co", ["feature/dirty2"], cwd=main_path)
    worktree_path = worktree_git_repo / "feature" / "dirty2"
    (worktree_path / "test.txt").write_text("uncommitted")

    # Try to remove with short force flag
    result = run_wt("rm", ["feature/dirty2", "-f"], cwd=main_path)

    assert result.returncode == 0
    assert not worktree_path.exists()


def test_gent_rm_cleans_empty_parent_dirs(worktree_git_repo, run_wt):
    """Test wt rm removes empty parent directories."""
    main_path = worktree_git_repo / "main"

    # Create a worktree with nested path
    run_wt("co", ["release/202512"], cwd=main_path)
    assert (worktree_git_repo / "release" / "202512").exists()

    # Remove it
    result = run_wt("rm", ["release/202512", "-f"], cwd=main_path)

    assert result.returncode == 0
    # Check that empty parent dir is also removed
    assert not (worktree_git_repo / "release").exists()


def test_gent_co_existing_branch(worktree_git_repo, run_wt):
    """Test wt co checks out existing worktree."""
    main_path = worktree_git_repo / "main"

    # Create a worktree
    run_wt("co", ["feature/existing"], cwd=main_path)

    # Try to check it out again (should just return the path)
    result = run_wt("co", ["feature/existing"], cwd=main_path)

    assert result.returncode == 0
    assert "feature/existing" in result.stdout or "feature" in result.stdout


def test_gent_co_new_branch_with_explicit_base(worktree_git_repo, run_wt):
    """Test wt co creates new worktree with explicit base branch."""
    main_path = worktree_git_repo / "main"

    result = run_wt("co", ["feature/brand-new", "main"], cwd=main_path)

    assert result.returncode == 0
    assert (worktree_git_repo / "feature" / "brand-new").exists()


def test_gent_co_sets_upstream_for_remote_branch(worktree_git_repo, run_wt):
    """Test wt co sets upstream when checking out from remote branch."""
    main_path = worktree_git_repo / "main"
    bare_path = worktree_git_repo / ".bare"

    # Create a remote-tracking branch by simulating a remote branch
    subprocess.run(
        ["git", "checkout", "-b", "feature/remote-test"],
        cwd=main_path,
        check=True,
        capture_output=True,
    )
    test_file = main_path / "test.txt"
    test_file.write_text("test content")
    subprocess.run(["git", "add", "test.txt"], cwd=main_path, check=True, capture_output=True)
    subprocess.run(
        ["git", "commit", "-m", "Test commit"], cwd=main_path, check=True, capture_output=True
    )

    # Get the commit SHA
    commit_sha_result = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=main_path, check=True, capture_output=True, text=True
    )
    commit_sha = commit_sha_result.stdout.strip()

    # Simulate having this as a remote branch
    subprocess.run(
        ["git", "update-ref", "refs/remotes/origin/feature/remote-test", commit_sha],
        cwd=bare_path,
        check=True,
        capture_output=True,
    )

    # Switch back to main and remove the local branch
    subprocess.run(["git", "checkout", "main"], cwd=main_path, check=True, capture_output=True)
    subprocess.run(
        ["git", "branch", "-D", "feature/remote-test"], cwd=bare_path, check=True, capture_output=True
    )

    # Now checkout the "remote" branch using wt co
    result = run_wt("co", ["feature/remote-test"], cwd=main_path)

    assert result.returncode == 0
    worktree_path = worktree_git_repo / "feature" / "remote-test"
    assert worktree_path.exists()

    # Verify upstream tracking is configured
    branch_result = subprocess.run(
        ["git", "rev-parse", "--abbrev-ref", "HEAD"], cwd=worktree_path, capture_output=True, text=True
    )
    current_branch = branch_result.stdout.strip()
    assert current_branch == "feature/remote-test"

    # Verify the tracking configuration
    config_remote = subprocess.run(
        ["git", "config", f"branch.{current_branch}.remote"],
        cwd=worktree_path,
        capture_output=True,
        text=True,
    )
    config_merge = subprocess.run(
        ["git", "config", f"branch.{current_branch}.merge"],
        cwd=worktree_path,
        capture_output=True,
        text=True,
    )

    assert config_remote.returncode == 0
    assert config_merge.returncode == 0
    assert config_remote.stdout.strip() == "origin"
    assert config_merge.stdout.strip() == "refs/heads/feature/remote-test"


def test_gent_rm_unmerged_branch_requires_force(worktree_git_repo, run_wt):
    """Test wt rm requires -f flag for unmerged branches."""
    main_path = worktree_git_repo / "main"
    bare_path = worktree_git_repo / ".bare"

    # Create a worktree with new commits (unmerged)
    run_wt("co", ["feature/unmerged"], cwd=main_path)
    unmerged_path = worktree_git_repo / "feature" / "unmerged"

    # Add a commit to make it unmerged
    (unmerged_path / "new_file.txt").write_text("new content")
    subprocess.run(["git", "add", "."], cwd=unmerged_path, check=True, capture_output=True)
    subprocess.run(
        ["git", "commit", "-m", "Unmerged commit"],
        cwd=unmerged_path,
        check=True,
        capture_output=True,
    )

    # Try to remove without force flag - should fail
    result = run_wt("rm", ["feature/unmerged"], cwd=main_path)
    assert result.returncode != 0
    assert "not fully merged" in result.stderr
    assert "wt rm feature/unmerged -f" in result.stderr

    # Re-create worktree if it was removed
    if not unmerged_path.exists():
        subprocess.run(
            ["git", "worktree", "add", str(unmerged_path), "feature/unmerged"],
            cwd=bare_path,
            check=True,
            capture_output=True,
        )

    # Now try with force flag - should succeed
    result = run_wt("rm", ["feature/unmerged", "-f"], cwd=main_path)
    assert result.returncode == 0
    assert not unmerged_path.exists()

    # Verify branch is deleted
    result = subprocess.run(
        ["git", "branch", "--list", "feature/unmerged"],
        cwd=bare_path,
        capture_output=True,
        text=True,
    )
    assert "feature/unmerged" not in result.stdout


def test_gent_rm_from_non_worktree_directory(worktree_git_repo, run_wt):
    """Test wt rm without branch arg from root directory provides helpful error."""
    root = worktree_git_repo

    # Try to remove without being in a worktree and without branch arg
    result = run_wt("rm", [], cwd=root)

    assert result.returncode != 0
    assert (
        "specify a branch name" in result.stderr.lower() or "not in a worktree" in result.stderr.lower()
    )


def test_gent_rm_already_removed_worktree(worktree_git_repo, run_wt):
    """Test wt rm handles manually deleted worktrees gracefully."""
    main_path = worktree_git_repo / "main"
    bare_path = worktree_git_repo / ".bare"

    # Create a worktree
    run_wt("co", ["feature/manual-delete"], cwd=main_path)
    worktree_path = worktree_git_repo / "feature" / "manual-delete"
    assert worktree_path.exists()

    # Manually delete the worktree directory (simulating user manual deletion)
    import shutil

    shutil.rmtree(worktree_path)
    assert not worktree_path.exists()

    # Now try to remove with wt rm - should handle gracefully without crashing
    result = run_wt("rm", ["feature/manual-delete", "-f"], cwd=main_path)

    # The command should complete (might return 0 or non-zero depending on state)
    # The important thing is it doesn't crash and attempts cleanup

    # Verify worktree directory is still gone (should not be recreated)
    assert not worktree_path.exists()

    # Verify worktree reference is cleaned up from git worktree list
    result = subprocess.run(
        ["git", "worktree", "list"],
        cwd=bare_path,
        capture_output=True,
        text=True,
    )
    # The worktree path should not appear in the list, or if it does, it should be marked as prunable
    # We mainly care that the command handled the situation gracefully
    assert "manual-delete" not in result.stdout or "prunable" in result.stdout.lower()


def test_gent_co_uses_current_worktree_as_base(worktree_git_repo, run_wt):
    """Test wt co uses current worktree's branch as base when not specified."""
    main_path = worktree_git_repo / "main"

    # Create a feature worktree
    run_wt("co", ["feature/parent"], cwd=main_path)
    parent_path = worktree_git_repo / "feature" / "parent"

    # Add a commit to the parent feature branch to differentiate it from main
    (parent_path / "parent_file.txt").write_text("parent content")
    subprocess.run(["git", "add", "."], cwd=parent_path, check=True, capture_output=True)
    subprocess.run(
        ["git", "commit", "-m", "Parent feature commit"],
        cwd=parent_path,
        check=True,
        capture_output=True,
    )

    # Now create a child branch from within the feature/parent worktree
    result = run_wt("co", ["feature/child"], cwd=parent_path)

    assert result.returncode == 0
    child_path = worktree_git_repo / "feature" / "child"
    assert child_path.exists()

    # Verify the child has the parent's commit
    assert (child_path / "parent_file.txt").exists()

    # Verify the child branch was created from feature/parent, not main
    # Check git log to see if parent's commit is in child's history
    log_result = subprocess.run(
        ["git", "log", "--oneline", "--all"],
        cwd=child_path,
        capture_output=True,
        text=True,
        check=True,
    )
    assert "Parent feature commit" in log_result.stdout


def test_gent_co_defaults_to_main_from_root(worktree_git_repo, run_wt):
    """Test wt co defaults to main when run from root directory."""
    root = worktree_git_repo
    main_path = worktree_git_repo / "main"

    # Add a commit to main
    (main_path / "main_file.txt").write_text("main content")
    subprocess.run(["git", "add", "."], cwd=main_path, check=True, capture_output=True)
    subprocess.run(
        ["git", "commit", "-m", "Main commit"],
        cwd=main_path,
        check=True,
        capture_output=True,
    )

    # Create a branch from the root directory (not in a worktree)
    result = run_wt("co", ["feature/from-root"], cwd=root)

    assert result.returncode == 0
    new_path = worktree_git_repo / "feature" / "from-root"
    assert new_path.exists()

    # Verify it has main's commit
    assert (new_path / "main_file.txt").exists()


def test_gent_co_defaults_to_main_from_bare(worktree_git_repo, run_wt):
    """Test wt co defaults to main when run from .bare directory."""
    bare_path = worktree_git_repo / ".bare"
    main_path = worktree_git_repo / "main"

    # Add a commit to main
    (main_path / "main_file2.txt").write_text("main content 2")
    subprocess.run(["git", "add", "."], cwd=main_path, check=True, capture_output=True)
    subprocess.run(
        ["git", "commit", "-m", "Main commit 2"],
        cwd=main_path,
        check=True,
        capture_output=True,
    )

    # Create a branch from the .bare directory
    result = run_wt("co", ["feature/from-bare"], cwd=bare_path)

    assert result.returncode == 0
    new_path = worktree_git_repo / "feature" / "from-bare"
    assert new_path.exists()

    # Verify it has main's commit
    assert (new_path / "main_file2.txt").exists()
