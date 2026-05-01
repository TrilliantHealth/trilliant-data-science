"""
Shared utilities for worktree management.
Used by multiple worktree commands.
"""

from __future__ import annotations

import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Literal, NoReturn, overload

from thds.gent import output
from thds.gent._repo import GENT_BARE_SETUP, GENT_REPO_PATH, github_tree_url

BARE_SETUP_URL = github_tree_url(path=f"{GENT_REPO_PATH}/{GENT_BARE_SETUP}")


@dataclass(frozen=True)
class WorktreeInfo:
    """Information about a git worktree."""

    path: Path
    relative: Path
    head: str | None = None
    branch: str | None = None
    bare: bool = False
    detached: bool = False


def find_worktree_root(start_path: Path | None = None) -> Path | None:
    """
    Find the worktree root by looking for a .bare directory.
    Searches from start_path up through parent directories.

    Args:
        start_path: Directory to start searching from (default: cwd)

    Returns:
        Path to the worktree root (parent of .bare) or None if not found
    """
    if start_path is None:
        start_path = Path.cwd()

    current = start_path.resolve()

    # Walk up the directory tree
    while current != current.parent:
        bare_dir = current / ".bare"
        if bare_dir.exists() and bare_dir.is_dir():
            return current
        current = current.parent

    # Check root directory
    bare_dir = current / ".bare"
    if bare_dir.exists() and bare_dir.is_dir():
        return current

    return None


def error_exit(message: str, code: int = 1) -> NoReturn:
    """Print error message to stderr and exit."""
    output.error(message, code=code)


def get_worktree_root_or_exit() -> Path:
    """
    Find worktree root or exit with error message.
    Convenience function for scripts that require a worktree root.

    Provides helpful guidance if user is in a regular git repo instead.
    """
    root = find_worktree_root()
    if root is None:
        # Check if we're in a regular git repository
        in_git_repo = False
        try:
            result = subprocess.run(
                ["git", "rev-parse", "--git-dir"],
                capture_output=True,
                text=True,
                check=True,
            )
            git_dir = result.stdout.strip()
            in_git_repo = True

            # If .git exists as a directory (not a gitdir file), we're in a regular repo
            if git_dir == ".git" or (Path(git_dir).is_dir() and not git_dir.endswith(".bare")):
                setup_link = output.link(BARE_SETUP_URL)
                error_exit(
                    f"Not in a worktree directory.\n\n"
                    f"You appear to be in a regular git repository.\n"
                    f"wt requires a bare repository structure with worktrees.\n\n"
                    f"To get started with wt:\n"
                    f"  1. Clone a new repository:\n"
                    f"     wt clone <url> [directory]\n\n"
                    f"  2. Or convert your existing repository:\n"
                    f"     {setup_link}"
                )
        except subprocess.CalledProcessError:
            # Not in a git repo at all
            pass

        # If we got here, we're either not in a git repo or in an unsupported setup
        if not in_git_repo:
            setup_link = output.link(BARE_SETUP_URL)
            error_exit(
                f"Not in a git repository.\n\n"
                f"wt requires a bare repository structure with worktrees.\n\n"
                f"To get started with wt:\n"
                f"  1. Clone a repository:\n"
                f"     wt clone <url> [directory]\n\n"
                f"  2. Or set up an existing repository:\n"
                f"     {setup_link}"
            )
        else:
            # In a git repo but not a recognized structure
            error_exit("Not in a worktree directory (no .bare directory found)")
    return root


def get_current_worktree_branch() -> str | None:
    """
    Get the branch name for the current worktree.
    Returns the relative path from root (e.g., "main", "release/202512").
    Works with both regular branches and detached HEAD states.

    Returns:
        Branch name or None if not in a worktree
    """
    root = find_worktree_root()
    if root is None:
        return None

    current_path = Path.cwd().resolve()

    # Check if we're under the root
    try:
        relative = current_path.relative_to(root)
    except ValueError:
        return None

    # If we're at the root itself, we're not in a worktree
    if relative == Path("."):
        return None

    # Parse git worktree list to find which worktree contains current path
    try:
        worktrees = parse_git_worktree_list(root)

        # Find which worktree contains the current path
        # Filter out bare worktrees
        for wt in worktrees:
            if wt.bare:
                continue

            try:
                current_path.relative_to(wt.path)
                # Current path is under this worktree
                # Return the relative path from root
                return str(wt.relative)
            except ValueError:
                continue

        return None

    except Exception:
        return None


def get_current_worktree_branch_or_exit() -> str:
    """
    Get the current worktree branch or exit with error message.
    Convenience function for scripts that require being in a worktree.
    """
    branch = get_current_worktree_branch()
    if branch is None:
        error_exit(
            "Not in a worktree directory. Run this command from within a worktree, or specify a branch name."
        )
    return branch


def get_bare_path(root: Path) -> Path:
    """
    Get the bare repository path from a worktree root.

    Args:
        root: Worktree root directory

    Returns:
        Path to the .bare directory
    """
    return root / ".bare"


def resolve_branch_argument(branch: str | None) -> str:
    """
    Resolve branch argument, using current worktree if not provided.

    Args:
        branch: Optional branch name argument

    Returns:
        The branch name

    Raises:
        SystemExit: If branch is None and not in a worktree
    """
    if branch is None:
        return get_current_worktree_branch_or_exit()
    return branch


def resolve_worktree_path(branch: str, must_exist: bool = False) -> Path:
    """
    Resolve a worktree path by branch name.

    Args:
        branch: Branch name (e.g., "main", "feature/test")
        must_exist: If True, exit with error if path doesn't exist

    Returns:
        Path to the worktree

    Raises:
        SystemExit: If must_exist is True and path doesn't exist
    """
    root = get_worktree_root_or_exit()
    worktree_path = root / branch

    if must_exist and not worktree_path.exists():
        error_exit(f"Worktree not found at {worktree_path}")

    return worktree_path


def ignore_git_errors(func: Callable[..., Any], *args: Any, **kwargs: Any) -> bool:
    """
    Execute a function, ignoring git command failures.

    Args:
        func: Function to execute (typically a git command)
        *args: Positional arguments to pass to func
        **kwargs: Keyword arguments to pass to func

    Returns:
        True if function succeeded, False if it raised CalledProcessError
    """
    try:
        func(*args, **kwargs)
        return True
    except subprocess.CalledProcessError:
        return False


def extract_subprocess_error(exc: subprocess.CalledProcessError) -> str:
    """
    Extract user-friendly error message from subprocess failure.

    Args:
        exc: The CalledProcessError exception

    Returns:
        Cleaned error message string
    """
    return exc.stderr.strip() if exc.stderr else str(exc)


@overload
def run_git(
    *args: str, cwd: Path | None = None, capture: Literal[True] = True, check: bool = True
) -> subprocess.CompletedProcess: ...


@overload
def run_git(
    *args: str, cwd: Path | None = None, capture: Literal[False] = False, check: bool = True
) -> bool: ...


def run_git(
    *args: str, cwd: Path | None = None, capture: bool = True, check: bool = True
) -> subprocess.CompletedProcess | bool:
    """
    Run a git command with consistent error handling.

    Args:
        *args: Git command arguments (e.g., "status", "fetch", "origin")
        cwd: Directory to run command in (default: current directory)
        capture: Whether to capture output (default: True)
        check: Whether to raise on non-zero exit (default: True)

    Returns:
        CompletedProcess if capture=True
        bool (success/failure) if capture=False
    """
    try:
        result = subprocess.run(["git", *args], cwd=cwd, capture_output=capture, text=True, check=check)
        if not capture:
            return result.returncode == 0
        return result
    except subprocess.CalledProcessError as e:
        if not capture:
            return False
        # Re-raise with better error message (check=False won't raise, so this is only for check=True)
        stderr = e.stderr.strip() if e.stderr else str(e)
        raise subprocess.CalledProcessError(e.returncode, e.cmd, e.stdout, stderr)


def run_git_streaming(*args: str, cwd: Path | None = None) -> None:
    """
    Run a git command and stream output to the terminal in real-time.

    Use this for long-running operations where users want to see progress
    (e.g., clone, fetch, push, worktree add).

    Args:
        *args: Git command arguments (e.g., "clone", "--bare", url, path)
        cwd: Directory to run command in (default: current directory)

    Raises:
        subprocess.CalledProcessError: If the command fails
    """
    # Simple approach: don't capture output, let it stream directly to terminal
    # The shell wrapper (wt.sh) must not capture output for this to work properly
    subprocess.run(["git", *args], cwd=cwd, check=True)


def git_branch_exists(ref: str, cwd: Path | None = None) -> bool:
    """
    Check if a git reference exists.

    Args:
        ref: Git reference (e.g., "refs/heads/main", "refs/remotes/origin/main")
        cwd: Directory to run command in

    Returns:
        True if reference exists, False otherwise
    """
    try:
        subprocess.run(
            ["git", "show-ref", "--verify", "--quiet", ref], cwd=cwd, check=True, capture_output=True
        )
        return True
    except subprocess.CalledProcessError:
        return False


def git_current_branch(cwd: Path) -> str | None:
    """
    Get the current branch name in a git directory.

    Args:
        cwd: Directory to check

    Returns:
        Branch name, "HEAD" for detached HEAD, or None on error
    """
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"],
            cwd=cwd,
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout.strip()
    except subprocess.CalledProcessError:
        return None


def git_checkout(branch: str, cwd: Path, fetch_first: bool = False) -> None:
    """
    Checkout a branch in a git directory.

    Args:
        branch: Branch name to checkout
        cwd: Directory to run command in
        fetch_first: Whether to fetch before checking out

    Raises:
        subprocess.CalledProcessError: If checkout fails
    """
    if fetch_first:
        try:
            subprocess.run(
                ["git", "fetch", "origin"], cwd=cwd, check=True, capture_output=True, text=True
            )
        except subprocess.CalledProcessError:
            pass  # Continue even if fetch fails

    subprocess.run(["git", "checkout", branch], cwd=cwd, check=True, capture_output=True, text=True)


def get_git_config(key: str, cwd: Path, default: str | None = None) -> str | None:
    """
    Get a git config value.

    Args:
        key: Git config key (e.g., "branch.main.remote")
        cwd: Working directory for git command
        default: Default value to return if key doesn't exist

    Returns:
        Config value as string, or default if not found
    """
    try:
        result = subprocess.run(
            ["git", "config", key],
            cwd=cwd,
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout.strip()
    except subprocess.CalledProcessError:
        return default


def set_git_config(key: str, value: str, cwd: Path) -> None:
    """
    Set a git config value.

    Args:
        key: Git config key (e.g., "branch.main.remote")
        value: Value to set
        cwd: Working directory for git command

    Raises:
        subprocess.CalledProcessError: If git config fails
    """
    subprocess.run(
        ["git", "config", key, value],
        cwd=cwd,
        check=True,
        capture_output=True,
        text=True,
    )


def get_branch_list(cwd: Path, remote: bool = False, strip_remote_prefix: bool = True) -> list[str]:
    """
    Get list of git branches.

    Args:
        cwd: Working directory for git command
        remote: If True, get remote branches; if False, get local branches
        strip_remote_prefix: If True, strip remote prefix (e.g., "origin/") from branch names

    Returns:
        List of branch names

    Raises:
        subprocess.CalledProcessError: If git command fails
    """
    # Build command functionally
    cmd = ["git", "branch", *(["-r"] if remote else []), "--format=%(refname:short)"]

    result = subprocess.run(
        cmd,
        cwd=cwd,
        capture_output=True,
        text=True,
        check=True,
    )

    branches = result.stdout.strip().split("\n")
    branches = [b for b in branches if b]  # Filter empty strings

    if remote and strip_remote_prefix:
        # Strip remote prefix (e.g., "origin/main" -> "main")
        return [branch.split("/", 1)[1] if "/" in branch else branch for branch in branches]

    return branches


def cleanup_empty_parent_dirs(path: Path, root: Path) -> None:
    """
    Remove empty parent directories up to root.

    Walks up the directory tree from path, removing empty directories
    until reaching the root or a non-empty directory.

    Args:
        path: Starting path to check parents from
        root: Root path to stop at (will not remove this)
    """
    parent = path.parent
    while parent != root and parent.exists():
        try:
            # Check if directory is empty
            if not any(parent.iterdir()):
                parent.rmdir()
                output.info(f"Removed empty directory: {parent}")
                parent = parent.parent
            else:
                # Directory not empty, stop
                break
        except OSError:
            # Permission error or other issue, stop
            break


def parse_git_worktree_list(root: Path) -> list[WorktreeInfo]:
    """
    Parse git worktree list --porcelain output.

    Args:
        root: Repository root path

    Returns:
        List of WorktreeInfo objects containing:
        - path: Absolute path to worktree
        - relative: Path relative to root (or Path('.bare') for bare repo)
        - head: Short hash of HEAD (first 10 chars) or None
        - branch: Branch name (without refs/heads/ prefix) or None
        - bare: Whether this is the bare repository
        - detached: Whether HEAD is detached

    Raises:
        subprocess.CalledProcessError: If git command fails
    """
    bare_path = get_bare_path(root)
    result = subprocess.run(
        ["git", "worktree", "list", "--porcelain"],
        cwd=bare_path,
        capture_output=True,
        text=True,
        check=True,
    )

    # Group lines into worktree records (separated by "worktree" lines)
    # Functional approach: fold over lines to build records
    lines = result.stdout.strip().split("\n")

    def group_records(lines: list[str]) -> list[list[str]]:
        """Group lines into worktree records functionally."""
        if not lines:
            return []

        records: list[list[str]] = []
        current: list[str] = []

        for line in lines:
            if line.startswith("worktree "):
                if current:
                    records = [*records, current]
                current = [line]
            else:
                current = [*current, line]

        return [*records, current] if current else records

    worktree_records = group_records(lines)

    # Parse each record into a WorktreeInfo (functional - no mutation)
    def parse_worktree_record(record: list[str]) -> WorktreeInfo:
        """Parse a worktree record into WorktreeInfo."""
        worktree_path = Path(record[0].split(" ", 1)[1])
        relative = worktree_path.relative_to(root) if worktree_path != root else Path(".bare")

        head = None
        branch = None
        bare = False
        detached = False

        for line in record[1:]:
            if line.startswith("HEAD "):
                head = line.split(" ", 1)[1][:10]
            elif line.startswith("branch "):
                branch_ref = line.split(" ", 1)[1]
                if branch_ref.startswith("refs/heads/"):
                    branch = branch_ref[len("refs/heads/") :]
            elif line == "bare":
                bare = True
            elif line == "detached":
                detached = True

        return WorktreeInfo(
            path=worktree_path, relative=relative, head=head, branch=branch, bare=bare, detached=detached
        )

    return [parse_worktree_record(record) for record in worktree_records]
