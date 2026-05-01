"""
Checkout an existing branch as a worktree or create a new one.
Used by the 'wt co' command.
"""

from __future__ import annotations

import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

import argh

from thds.gent import output
from thds.gent.hooks import hook_exists, run_hook
from thds.gent.readme import ensure_readme
from thds.gent.shell_check import warn_if_no_shell_wrapper
from thds.gent.utils import (
    error_exit,
    extract_subprocess_error,
    get_bare_path,
    get_current_worktree_branch,
    get_git_config,
    get_worktree_root_or_exit,
    git_branch_exists,
    git_checkout,
    git_current_branch,
    ignore_git_errors,
    run_git,
    run_git_streaming,
    set_git_config,
)


def handle_existing_worktree(branch: str, worktree_path: Path) -> None:
    """Handle the case where worktree already exists.

    Either prints path if already on correct branch, or checks out the branch.
    Exits the program after handling.
    """
    current_branch = git_current_branch(worktree_path)

    if not current_branch:
        error_exit(f"Failed to check worktree state at {worktree_path}")

    # Check if we're on the correct branch
    if current_branch == branch:
        # Already on correct branch
        output.info(f"Worktree already exists at {worktree_path}")
        print(worktree_path)
        warn_if_no_shell_wrapper()
        sys.exit(0)

    # Need to checkout the branch (either from detached HEAD or different branch)
    if current_branch == "HEAD":
        output.info("Worktree exists but in detached HEAD state")
    else:
        output.info(f"Worktree exists but on branch '{current_branch}'")

    output.info(f"Fetching and checking out {branch}...")

    try:
        git_checkout(branch, worktree_path, fetch_first=True)
        output.info(f"Checked out {branch}")
        print(worktree_path)
        warn_if_no_shell_wrapper()
        sys.exit(0)
    except subprocess.CalledProcessError as e:
        error_exit(f"Failed to checkout {branch} in existing worktree: {extract_subprocess_error(e)}")


def determine_branch_target(branch: str, base_branch: str, bare_path: Path) -> tuple[str, bool]:
    """Determine which branch target to use and whether to create a new branch.

    Returns:
        Tuple of (target_ref, create_new_branch)
    """
    if git_branch_exists(f"refs/heads/{branch}", cwd=bare_path):
        # Local branch exists
        output.info(f"✓ Found existing local branch '{branch}'")
        output.info("→ Checking out as worktree")
        return (branch, False)

    if git_branch_exists(f"refs/remotes/origin/{branch}", cwd=bare_path):
        # Remote branch exists - need to create local branch from it
        output.info(f"✓ Found existing remote branch 'origin/{branch}'")
        output.info("→ Creating local tracking branch")
        return (f"origin/{branch}", True)

    # Create new branch from base (base_branch is always provided, defaults to HEAD)
    output.info(f"✗ Branch '{branch}' not found locally or remotely")
    output.info(f"→ Creating new branch from '{base_branch}'")
    return (base_branch, True)


@dataclass(frozen=True)
class UpstreamConfig:
    """Configuration for setting upstream branch tracking."""

    should_set: bool
    remote_name: str | None = None
    branch_name: str | None = None


def should_set_upstream_branch(
    branch: str, target: str, create_new_branch: bool, worktree_path: Path, bare_path: Path
) -> UpstreamConfig:
    """Determine if upstream tracking should be set.

    Returns:
        UpstreamConfig with tracking configuration
    """
    if target.startswith("origin/"):
        # We checked out from remote, definitely set upstream
        return UpstreamConfig(should_set=True, remote_name="origin", branch_name=branch)

    if not create_new_branch and git_branch_exists(f"refs/remotes/origin/{branch}", cwd=bare_path):
        # Local branch exists and corresponding remote branch exists
        # Check if upstream is already set
        upstream_remote = get_git_config(f"branch.{branch}.remote", worktree_path)
        if upstream_remote is None:
            # No upstream set
            return UpstreamConfig(should_set=True, remote_name="origin", branch_name=branch)

    return UpstreamConfig(should_set=False)


def set_upstream_tracking(
    branch: str, upstream_remote: str, upstream_branch: str, worktree_path: Path
) -> None:
    """Set upstream tracking information for a branch."""
    try:
        # Set tracking information using git config
        set_git_config(f"branch.{branch}.remote", upstream_remote, worktree_path)
        set_git_config(f"branch.{branch}.merge", f"refs/heads/{upstream_branch}", worktree_path)
        output.info(f"Set upstream to {upstream_remote}/{upstream_branch}")
    except subprocess.CalledProcessError as e:
        # Non-fatal - worktree was created successfully
        output.warning(f"Failed to set upstream branch: {extract_subprocess_error(e)}")


@argh.arg("branch", help="Branch name to checkout or create")
@argh.arg(
    "base_branch",
    nargs="?",
    default="-",
    help="Base branch for creating new branches (default: current worktree or main)",
)
def main(branch: str, base_branch: str | None) -> None:
    """Checkout an existing branch as a worktree or create a new one.

    Decision logic (checked in order):
      1. If branch exists locally → checkout as worktree
      2. If branch exists on remote → checkout and track remote
      3. Otherwise → create new branch from base

    Examples:
      wt co feature/existing        # Checkout existing (local or remote)
      wt co feature/new             # Create new from HEAD (current worktree or main)
      wt co feature/new main        # Create new from main explicitly

    Base branch behavior (when creating new):
      - No base specified: Uses current worktree branch, or 'main' if not in worktree
      - Base specified: Uses the specified branch as starting point

    Note: Always fetches from remote first to check for remote branches.
    """
    root = get_worktree_root_or_exit()
    worktree_path = root / branch

    # Check if worktree already exists
    if worktree_path.exists():
        handle_existing_worktree(branch, worktree_path)
        # Note: handle_existing_worktree exits the program

    # Worktree doesn't exist - need to create it
    # Fetch from remote to ensure we have latest branch info (if remote exists)
    output.info("Fetching latest branch info from remote...")
    bare_path = get_bare_path(root)
    # Ignore fetch errors - no remote or fetch failure is okay
    ignore_git_errors(run_git, "fetch", "origin", cwd=bare_path, capture=False)

    # Determine base branch if not provided
    if base_branch is None or base_branch == "-":
        # Check if we're in a worktree - if so, use that branch as base
        current_worktree = get_current_worktree_branch()
        if current_worktree:
            base_branch = current_worktree
            output.info(f"Base branch: '{current_worktree}' (current worktree)")
        else:
            # Not in a worktree (e.g., in root or .bare directory)
            # Default to main
            base_branch = "main"
            output.info("Base branch: 'main' (default)")
    else:
        output.info(f"Base branch: '{base_branch}' (explicit)")

    # Determine which branch to use and how to create the worktree
    target, create_new_branch = determine_branch_target(branch, base_branch, bare_path)

    # Create parent directory if needed
    worktree_path.parent.mkdir(parents=True, exist_ok=True)

    # Add the worktree
    try:
        # If creating a new branch, use -b flag to specify the new branch name
        args = (
            "worktree",
            "add",
            *(["-b", branch] if create_new_branch else []),
            str(worktree_path),
            target,
        )

        run_git_streaming(*args, cwd=bare_path)

        # Set upstream branch if appropriate
        upstream_config = should_set_upstream_branch(
            branch, target, create_new_branch, worktree_path, bare_path
        )

        if upstream_config.should_set and upstream_config.remote_name and upstream_config.branch_name:
            set_upstream_tracking(
                branch, upstream_config.remote_name, upstream_config.branch_name, worktree_path
            )

        # Success message
        if create_new_branch and target.startswith("origin/"):
            output.success(f"Created local tracking branch '{branch}' from remote")
        elif create_new_branch:
            output.success(f"Created new branch '{branch}' from '{base_branch}'")
        else:
            output.success(f"Checked out existing branch '{branch}' as worktree")

        # Auto-run init on new worktree creation
        if hook_exists("init", worktree_path):
            run_hook("init", worktree_path, branch)

        print(worktree_path)
        warn_if_no_shell_wrapper()
        ensure_readme()
    except subprocess.CalledProcessError as e:
        error_exit(f"Failed to create worktree: {e}")


if __name__ == "__main__":
    argh.dispatch_command(main)
