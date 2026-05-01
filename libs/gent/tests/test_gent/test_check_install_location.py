"""Tests for check_install_location.py script."""

import subprocess
from pathlib import Path


def test_check_from_main_worktree(worktree_git_repo):
    """Test that check passes when installing from main worktree."""
    # Create libs/gent structure in main
    gent_path = worktree_git_repo / "main" / "libs" / "gent"
    gent_path.mkdir(parents=True)

    # Copy the check script to the location
    check_script = Path(__file__).parent.parent.parent / "check_install_location.py"
    target_script = gent_path / "check_install_location.py"
    target_script.write_text(check_script.read_text())

    # Run the check from main/libs/gent
    result = subprocess.run(
        ["python3", str(target_script)],
        cwd=gent_path,
        capture_output=True,
        text=True,
    )

    # Should exit 0 (success) - no warning
    assert result.returncode == 0
    assert result.stdout == ""


def test_check_from_feature_worktree(worktree_git_repo):
    """Test that check warns when installing from non-main worktree."""
    # Create a feature worktree
    bare_path = worktree_git_repo / ".bare"
    feature_path = worktree_git_repo / "feature" / "test-feature"

    subprocess.run(
        ["git", "worktree", "add", "-b", "test-feature", str(feature_path), "main"],
        cwd=bare_path,
        check=True,
        capture_output=True,
    )

    # Create libs/gent structure in feature worktree
    gent_path = feature_path / "libs" / "gent"
    gent_path.mkdir(parents=True)

    # Copy the check script
    check_script = Path(__file__).parent.parent.parent / "check_install_location.py"
    target_script = gent_path / "check_install_location.py"
    target_script.write_text(check_script.read_text())

    # Run the check from feature/test-feature/libs/gent
    result = subprocess.run(
        ["python3", str(target_script)],
        cwd=gent_path,
        capture_output=True,
        text=True,
    )

    # Should exit 1 (warning) and print WARNING|path
    assert result.returncode == 1
    assert result.stdout.startswith("WARNING|")

    # Extract path from WARNING|path
    warning_path = Path(result.stdout.split("|")[1].strip()).resolve()
    expected_path = (worktree_git_repo / "main" / "libs" / "gent").resolve()
    assert warning_path == expected_path


def test_check_from_regular_repo(regular_git_repo):
    """Test that check passes for regular (non-worktree) repos."""
    # Create libs/gent structure
    gent_path = regular_git_repo / "libs" / "gent"
    gent_path.mkdir(parents=True)

    # Copy the check script
    check_script = Path(__file__).parent.parent.parent / "check_install_location.py"
    target_script = gent_path / "check_install_location.py"
    target_script.write_text(check_script.read_text())

    # Run the check
    result = subprocess.run(
        ["python3", str(target_script)],
        cwd=gent_path,
        capture_output=True,
        text=True,
    )

    # Should exit 0 (success) - not in worktree structure, so no warning
    assert result.returncode == 0
    assert result.stdout == ""


def test_check_from_nested_path_in_main(worktree_git_repo):
    """Test that check passes when in nested path under main."""
    # Create libs/gent/src structure in main
    gent_path = worktree_git_repo / "main" / "libs" / "gent" / "src"
    gent_path.mkdir(parents=True)

    # Copy the check script to the gent directory (not src)
    check_script = Path(__file__).parent.parent.parent / "check_install_location.py"
    target_script = gent_path.parent / "check_install_location.py"
    target_script.write_text(check_script.read_text())

    # Run the check from src directory (but script is in parent)
    result = subprocess.run(
        ["python3", str(target_script)],
        cwd=gent_path,
        capture_output=True,
        text=True,
    )

    # Should exit 0 (success) - script location is in main/libs/gent
    assert result.returncode == 0
    assert result.stdout == ""


def test_check_from_non_standard_worktree_name(worktree_git_repo):
    """Test warning for worktree with non-standard name at root level."""
    # Create a worktree at root level (not in a subdirectory like feature/)
    bare_path = worktree_git_repo / ".bare"
    custom_path = worktree_git_repo / "my-branch"

    subprocess.run(
        ["git", "worktree", "add", "-b", "my-branch", str(custom_path), "main"],
        cwd=bare_path,
        check=True,
        capture_output=True,
    )

    # Create libs/gent structure
    gent_path = custom_path / "libs" / "gent"
    gent_path.mkdir(parents=True)

    # Copy the check script
    check_script = Path(__file__).parent.parent.parent / "check_install_location.py"
    target_script = gent_path / "check_install_location.py"
    target_script.write_text(check_script.read_text())

    # Run the check
    result = subprocess.run(
        ["python3", str(target_script)],
        cwd=gent_path,
        capture_output=True,
        text=True,
    )

    # Should exit 1 (warning) - not in main
    assert result.returncode == 1
    assert "WARNING|" in result.stdout
