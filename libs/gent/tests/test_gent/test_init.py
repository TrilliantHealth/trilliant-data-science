from thds.gent import __version__


def test_version_at_import():
    assert __version__


def test_init_calls_gent_init_script(worktree_git_repo, run_wt):
    """Test that wt init calls the .gent/init script with correct environment."""
    # Create a worktree
    main_path = worktree_git_repo / "main"
    run_wt("co", ["feature/init-test"], cwd=main_path)
    worktree_path = worktree_git_repo / "feature" / "init-test"

    # Create a simple .gent/init script that logs what it received
    gent_dir = worktree_path / ".gent"
    gent_dir.mkdir(parents=True)
    init_script = gent_dir / "init"
    init_script.write_text(
        """#!/bin/bash
set -euo pipefail
echo "WORKTREE_PATH=$GENT_WORKTREE_PATH"
echo "BRANCH=$GENT_BRANCH"
echo "SUCCESS"
"""
    )
    init_script.chmod(0o755)

    # Run wt init
    result = run_wt("init", ["feature/init-test"], cwd=main_path)

    # Verify it succeeded
    assert result.returncode == 0
    assert "SUCCESS" in result.stdout
    assert str(worktree_path) in result.stdout
    assert "feature/init-test" in result.stdout


def test_init_fails_if_no_gent_init_script(worktree_git_repo, run_wt):
    """Test that wt init fails with helpful error if .gent/init doesn't exist."""
    # Create a worktree without .gent/init
    main_path = worktree_git_repo / "main"
    run_wt("co", ["feature/no-init"], cwd=main_path)

    # Run wt init - should fail
    result = run_wt("init", ["feature/no-init"], cwd=main_path)

    # Verify it failed with helpful message
    assert result.returncode != 0
    assert ".gent/init" in result.stderr
    assert "no initialization script found" in result.stderr.lower()


def test_init_fails_if_script_not_executable(worktree_git_repo, run_wt):
    """Test that wt init fails if .gent/init is not executable."""
    # Create a worktree with non-executable .gent/init
    main_path = worktree_git_repo / "main"
    run_wt("co", ["feature/not-exec"], cwd=main_path)
    worktree_path = worktree_git_repo / "feature" / "not-exec"

    gent_dir = worktree_path / ".gent"
    gent_dir.mkdir(parents=True)
    init_script = gent_dir / "init"
    init_script.write_text("#!/bin/bash\necho 'test'\n")
    # Don't make it executable

    # Run wt init - should fail
    result = run_wt("init", ["feature/not-exec"], cwd=main_path)

    # Verify it failed with helpful message
    assert result.returncode != 0
    assert "not executable" in result.stderr
    assert "chmod +x" in result.stderr
