"""
Shared test utilities and fixtures for gent tests.
"""

import os
import shutil
import subprocess
import tempfile
from datetime import datetime
from pathlib import Path

import pytest


# Pure functions for git operations in tests
def git_run(path: Path, *args: str, check: bool = True, **kwargs):
    """Run a git command in a repository."""
    return subprocess.run(
        ["git", *args], cwd=path, capture_output=True, text=True, check=check, **kwargs
    )


def git_init(path: Path) -> None:
    """Initialize a git repository."""
    git_run(path, "init")


def git_config(path: Path, key: str, value: str) -> None:
    """Set a git config value."""
    git_run(path, "config", key, value)


def git_add(path: Path, *files: str) -> None:
    """Stage files in a repository."""
    git_run(path, "add", *files)


def git_commit(path: Path, message: str, date: datetime | None = None) -> None:
    """Create a commit, optionally with a specific date."""
    if date:
        timestamp = str(int(date.timestamp()))
        env = {"GIT_AUTHOR_DATE": timestamp, "GIT_COMMITTER_DATE": timestamp}
        full_env = {**os.environ, **env}
        subprocess.run(
            ["git", "commit", "-m", message],
            cwd=path,
            capture_output=True,
            text=True,
            check=True,
            env=full_env,
        )
    else:
        git_run(path, "commit", "-m", message)


def git_checkout(path: Path, *args: str) -> None:
    """Checkout a branch."""
    git_run(path, "checkout", *args)


def git_branch(path: Path, *args: str) -> None:
    """Run git branch command."""
    git_run(path, "branch", *args)


def write_file(path: Path, filename: str, content: str) -> None:
    """Write a file in a repository."""
    file_path = path / filename
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_text(content)


def git_remote_add(path: Path, name: str, url: str) -> None:
    """Add a remote to a repository."""
    git_run(path, "remote", "add", name, url)


def git_push(path: Path, *args: str) -> None:
    """Push to a remote."""
    git_run(path, "push", *args)


def init_repo_with_commit(
    path: Path,
    files: list[tuple[str, str]],
    commit_msg: str = "Initial commit",
    branch: str | None = None,
) -> None:
    """
    Initialize a repo, configure user, add files, and create initial commit.

    Args:
        path: Directory to initialize
        files: List of (filename, content) tuples
        commit_msg: Commit message
        branch: If provided, rename branch to this name after commit
    """
    git_init(path)
    git_config(path, "user.email", "test@example.com")
    git_config(path, "user.name", "Test User")
    for filename, content in files:
        write_file(path, filename, content)
    git_add(path, ".")
    git_commit(path, commit_msg)
    if branch:
        git_branch(path, "-M", branch)


def create_bare_remote_repo(
    tmpdir: Path, branches: dict[str, list[tuple[str, str]]] | None = None
) -> Path:
    """
    Create a bare git repository simulating a remote.

    Args:
        tmpdir: Temporary directory to create repo in
        branches: Dict of {branch_name: [(filename, content), ...]}
                 If None, creates main branch with README.md

    Returns:
        Path to the bare repository
    """
    # Default to main branch with README
    if branches is None:
        branches = {"main": [("README.md", "# Test Repository\n")]}

    remote_path = tmpdir / "remote-repo.git"
    remote_path.mkdir()

    # Initialize as bare repository
    subprocess.run(
        ["git", "init", "--bare"],
        cwd=remote_path,
        check=True,
        capture_output=True,
    )

    # Create a temporary repo to push initial content
    temp_repo = tmpdir / "temp-push"
    temp_repo.mkdir()

    # Create branches with their content
    first_branch = True
    for branch_name, files in branches.items():
        if first_branch:
            # First branch: init repo with initial commit then rename
            init_repo_with_commit(temp_repo, files, f"Initial commit on {branch_name}", branch_name)
            first_branch = False
        else:
            # Subsequent branches: checkout -b from first branch
            git_checkout(temp_repo, "-b", branch_name)
            for filename, content in files:
                write_file(temp_repo, filename, content)
            git_add(temp_repo, ".")
            git_commit(temp_repo, f"Add content for {branch_name}")

    # Add remote and push all branches
    git_remote_add(temp_repo, "origin", str(remote_path))
    for branch_name in branches.keys():
        git_push(temp_repo, "-u", "origin", branch_name)

    # Set HEAD to point to first branch
    first_branch_name = next(iter(branches.keys()))
    subprocess.run(
        ["git", "symbolic-ref", "HEAD", f"refs/heads/{first_branch_name}"],
        cwd=remote_path,
        check=True,
        capture_output=True,
    )

    # Clean up temp repo
    shutil.rmtree(temp_repo)

    return remote_path


def create_worktree_git_repo(tmpdir: Path) -> Path:
    """
    Create a git repository with bare worktree setup.

    Args:
        tmpdir: Temporary directory to create repo in

    Returns:
        Path to the worktree root (parent of .bare)
    """
    repo_path = tmpdir / "test-repo"
    repo_path.mkdir()

    # Create a temporary directory for initial setup
    temp_init = tmpdir / "init"
    temp_init.mkdir()

    # DEBUG: Check git version and initial branch config
    print("\n[DEBUG] Git version:")
    git_version = subprocess.run(["git", "--version"], capture_output=True, text=True, check=True)
    print(f"[DEBUG]   {git_version.stdout.strip()}")

    init_default = subprocess.run(
        ["git", "config", "init.defaultBranch"],
        cwd=temp_init,
        capture_output=True,
        text=True,
    )
    print(f"[DEBUG] init.defaultBranch config: '{init_default.stdout.strip()}'")

    # Check current branch after init (before init_repo_with_commit)
    current_branch_before = subprocess.run(
        ["git", "branch", "--show-current"],
        cwd=temp_init,
        capture_output=True,
        text=True,
        check=False,
    )
    print(f"[DEBUG] Current branch before init: '{current_branch_before.stdout.strip()}'")

    # Initialize and create initial commit with main branch
    init_repo_with_commit(
        temp_init,
        [("README.md", "# Test Repo\n")],
        "Initial commit",
        "main",
    )

    # Verify branch was renamed
    branch_after_rename = subprocess.run(
        ["git", "branch", "--show-current"],
        cwd=temp_init,
        capture_output=True,
        text=True,
        check=True,
    )
    print(f"[DEBUG] Current branch after init_repo_with_commit: '{branch_after_rename.stdout.strip()}'")

    # Move .git to .bare and make it bare
    bare_path = repo_path / ".bare"
    shutil.move(str(temp_init / ".git"), str(bare_path))
    subprocess.run(
        ["git", "config", "--bool", "core.bare", "true"],
        cwd=bare_path,
        check=True,
        capture_output=True,
    )

    # List all branches in bare repo
    bare_branches = subprocess.run(
        ["git", "branch", "-a"],
        cwd=bare_path,
        capture_output=True,
        text=True,
        check=True,
    )
    print(f"[DEBUG] Branches in bare repo:\n{bare_branches.stdout}")

    # Create main worktree
    main_path = repo_path / "main"
    print(f"[DEBUG] Creating worktree at: {main_path}")
    print(f"[DEBUG] Command: git worktree add {main_path} main")

    result = subprocess.run(
        ["git", "worktree", "add", str(main_path), "main"],
        cwd=bare_path,
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        print("[DEBUG] ERROR: git worktree add failed!")
        print(f"[DEBUG] Return code: {result.returncode}")
        print(f"[DEBUG] stdout: {result.stdout}")
        print(f"[DEBUG] stderr: {result.stderr}")
        result.check_returncode()  # Raise the error
    else:
        print("[DEBUG] Worktree created successfully")

    return repo_path


def create_regular_git_repo(tmpdir: Path) -> Path:
    """
    Create a regular (non-bare) git repository.

    Args:
        tmpdir: Temporary directory to create repo in

    Returns:
        Path to the repository directory
    """
    repo_path = tmpdir / "regular-repo"
    repo_path.mkdir()

    init_repo_with_commit(repo_path, [("README.md", "# Regular Repo\n")])

    return repo_path


@pytest.fixture
def worktree_git_repo():
    """
    Pytest fixture providing a git repository with worktree setup.

    Yields:
        Path to the worktree root directory
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        repo_path = create_worktree_git_repo(Path(tmpdir))
        yield repo_path


@pytest.fixture
def regular_git_repo():
    """
    Pytest fixture providing a regular (non-bare) git repository.

    Yields:
        Path to the repository directory
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        repo_path = create_regular_git_repo(Path(tmpdir))
        yield repo_path


@pytest.fixture
def run_wt():
    """Fixture that provides a function to run wt commands.

    Returns:
        Function that runs wt command and returns CompletedProcess
    """

    def _run_wt(command: str, args: list[str], cwd: Path | None = None):
        """Run a wt command and return the result.

        Args:
            command: The wt subcommand to run (e.g., "co", "rm", "list")
            args: List of arguments to pass to the command
            cwd: Working directory to run the command in

        Returns:
            CompletedProcess with stdout, stderr, and returncode
        """
        cmd = ["wt", command] + args
        result = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)
        return result

    return _run_wt


@pytest.fixture
def run_completion():
    """Fixture that provides a function to run wt-completion commands.

    Returns:
        Function that runs wt-completion and returns CompletedProcess
    """

    def _run_completion(*args, cwd: Path | None = None):
        """Run wt-completion command and return the result.

        Args:
            *args: Arguments to pass to wt-completion (e.g., "--worktrees", "--with-descriptions")
            cwd: Working directory to run the command in

        Returns:
            CompletedProcess with stdout, stderr, and returncode
        """
        cmd = ["wt-completion", *args]
        result = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)
        return result

    return _run_completion


@pytest.fixture
def remote_repo(tmp_path):
    """
    Pytest fixture providing a bare git repository simulating a remote.

    Creates a repository with a single main branch containing README.md.

    Yields:
        Path to the bare repository
    """
    return create_bare_remote_repo(tmp_path)


@pytest.fixture
def remote_repo_with_develop(tmp_path):
    """
    Pytest fixture providing a bare git repository with main and develop branches.

    Yields:
        Path to the bare repository
    """
    return create_bare_remote_repo(
        tmp_path,
        {
            "main": [("README.md", "# Test Repository\n")],
            "develop": [("DEVELOP.md", "# Development\n")],
        },
    )
