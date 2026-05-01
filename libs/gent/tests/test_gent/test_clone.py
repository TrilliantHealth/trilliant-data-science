"""
Tests for the wt clone command.
"""

import subprocess


def test_clone_basic(remote_repo, run_wt, tmp_path):
    """Test basic cloning works end-to-end."""
    # Change to tmp_path where we'll clone
    clone_dir = tmp_path / "clone-test"
    clone_dir.mkdir()

    result = run_wt("clone", [str(remote_repo), "--directory", "my-repo"], cwd=clone_dir)

    assert result.returncode == 0, f"Clone failed: {result.stderr}"
    assert "Successfully cloned repository" in result.stdout

    # Verify structure
    repo_path = clone_dir / "my-repo"
    assert repo_path.exists()
    assert (repo_path / ".bare").exists()
    assert (repo_path / "main").exists()
    assert (repo_path / "main" / "README.md").exists()
    assert (repo_path / "main" / "README.md").read_text() == "# Test Repository\n"


def test_clone_directory_exists_error(remote_repo, run_wt, tmp_path):
    """Test that cloning fails gracefully when directory already exists."""
    clone_dir = tmp_path / "clone-error-test"
    clone_dir.mkdir()

    # Create the target directory
    target = clone_dir / "my-repo"
    target.mkdir()

    result = run_wt("clone", [str(remote_repo), "--directory", "my-repo"], cwd=clone_dir)

    assert result.returncode != 0
    assert "already exists" in result.stderr.lower()


def test_clone_with_custom_directory(remote_repo, run_wt, tmp_path):
    """Test cloning with --directory flag."""
    clone_dir = tmp_path / "clone-custom-dir"
    clone_dir.mkdir()

    result = run_wt("clone", [str(remote_repo), "--directory", "custom-name"], cwd=clone_dir)

    assert result.returncode == 0
    assert (clone_dir / "custom-name").exists()
    assert (clone_dir / "custom-name" / ".bare").exists()
    assert (clone_dir / "custom-name" / "main").exists()


def test_clone_with_custom_branch(remote_repo_with_develop, run_wt, tmp_path):
    """Test cloning with --branch flag."""
    clone_dir = tmp_path / "clone-branch-test"
    clone_dir.mkdir()

    result = run_wt(
        "clone",
        [str(remote_repo_with_develop), "--directory", "dev-repo", "--branch", "develop"],
        cwd=clone_dir,
    )

    assert result.returncode == 0
    assert (clone_dir / "dev-repo" / "develop").exists()
    assert (clone_dir / "dev-repo" / "develop" / "DEVELOP.md").exists()
    # Main branch should not be checked out
    assert not (clone_dir / "dev-repo" / "main").exists()


def test_clone_cleanup_on_failure(run_wt, tmp_path):
    """Test that cleanup happens when clone fails."""
    clone_dir = tmp_path / "clone-cleanup-test"
    clone_dir.mkdir()

    # Try to clone from non-existent URL
    bad_url = "file:///nonexistent/repo.git"
    result = run_wt("clone", [bad_url, "--directory", "should-not-exist"], cwd=clone_dir)

    assert result.returncode != 0

    # Verify cleanup happened - directory should not exist
    assert not (clone_dir / "should-not-exist").exists()


def test_determine_default_branch_fallbacks(remote_repo, run_wt, tmp_path):
    """Test that default branch determination works with fallback strategies."""
    clone_dir = tmp_path / "clone-fallback-test"
    clone_dir.mkdir()

    # Remove symbolic-ref to test fallback
    subprocess.run(
        ["git", "symbolic-ref", "--delete", "HEAD"],
        cwd=remote_repo,
        check=False,
        capture_output=True,
    )

    # Clone should still work using ls-remote fallback
    result = run_wt("clone", [str(remote_repo), "--directory", "fallback-repo"], cwd=clone_dir)

    assert result.returncode == 0
    assert (clone_dir / "fallback-repo" / "main").exists()


def test_clone_with_branch_not_existing(remote_repo, run_wt, tmp_path):
    """Test that cloning fails gracefully when specified branch doesn't exist."""
    clone_dir = tmp_path / "clone-bad-branch-test"
    clone_dir.mkdir()

    # Try to clone with non-existent branch
    result = run_wt(
        "clone",
        [str(remote_repo), "--directory", "bad-branch-repo", "--branch", "nonexistent"],
        cwd=clone_dir,
    )

    assert result.returncode != 0
    # Verify cleanup happened - directory should not exist
    assert not (clone_dir / "bad-branch-repo").exists()


def test_clone_invalid_url(run_wt, tmp_path):
    """Test that cloning fails gracefully with invalid URLs."""
    clone_dir = tmp_path / "clone-invalid-url-test"
    clone_dir.mkdir()

    # Try to clone with malformed URL
    invalid_urls = [
        "not-a-url",
        "ftp://invalid-protocol.git",
        "",
        "file:///path/with spaces/repo.git",
    ]

    for bad_url in invalid_urls:
        result = run_wt("clone", [bad_url, "--directory", "invalid-repo"], cwd=clone_dir)

        # Should fail
        assert result.returncode != 0
        # Verify cleanup happened
        assert not (clone_dir / "invalid-repo").exists()


def test_integration_clone_then_co(remote_repo, run_wt, tmp_path):
    """Test the most common workflow: clone a repo then create a feature branch."""
    clone_dir = tmp_path / "integration-test"
    clone_dir.mkdir()

    # Step 1: Clone the repository
    result = run_wt("clone", [str(remote_repo), "--directory", "my-project"], cwd=clone_dir)
    assert result.returncode == 0

    repo_path = clone_dir / "my-project"
    assert repo_path.exists()
    assert (repo_path / ".bare").exists()
    assert (repo_path / "main").exists()

    # Step 2: Create a feature branch from main worktree
    main_path = repo_path / "main"
    result = run_wt("co", ["feature/new-work"], cwd=main_path)
    assert result.returncode == 0

    # Verify the feature worktree was created
    feature_path = repo_path / "feature" / "new-work"
    assert feature_path.exists()
    assert (feature_path / "README.md").exists()

    # Verify we have two worktrees now
    result = run_wt("list", [], cwd=main_path)
    assert result.returncode == 0
    assert "main" in result.stdout
    assert "feature/new-work" in result.stdout
