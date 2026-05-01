"""
Remove a git worktree.
Used by the 'wt rm' command.
"""

import subprocess
from pathlib import Path

import argh

from thds.gent import output
from thds.gent.readme import ensure_readme
from thds.gent.utils import (
    cleanup_empty_parent_dirs,
    error_exit,
    get_bare_path,
    get_worktree_root_or_exit,
    resolve_branch_argument,
    resolve_worktree_path,
    run_git,
)


def check_branch_can_be_deleted(branch: str, bare_path: Path, force: bool = False) -> None:
    """
    Check if a branch can be deleted, exit early if not.

    Mirrors the behavior of 'git branch -d' which checks if the branch is
    fully merged into HEAD or its upstream branch.

    Args:
        branch: Branch name to check
        bare_path: Path to bare repository
        force: If True, skip the check (can always force delete)

    Raises:
        SystemExit: If branch is unmerged and force is False
    """
    if force:
        # Skip check if force is enabled
        return

    # Check if branch exists
    result = run_git("branch", "--list", branch, cwd=bare_path, check=False)
    if not result.stdout.strip():
        # Branch doesn't exist, nothing to check
        return

    # Check if branch is merged into HEAD (current branch)
    # This mirrors what 'git branch -d' does
    result = run_git("merge-base", "--is-ancestor", branch, "HEAD", cwd=bare_path, check=False)
    if result.returncode == 0:
        # Branch is merged into HEAD
        return

    # Check if branch has an upstream and is merged into that
    result = run_git("rev-parse", "--abbrev-ref", f"{branch}@{{upstream}}", cwd=bare_path, check=False)
    if result.returncode == 0 and result.stdout.strip():
        upstream = result.stdout.strip()
        result = run_git("merge-base", "--is-ancestor", branch, upstream, cwd=bare_path, check=False)
        if result.returncode == 0:
            # Branch is merged into its upstream
            return

    # Branch is not fully merged
    output.error_multiline(
        f"The branch '{branch}' is not fully merged.",
        "If you are sure you want to delete it, run:",
        f"  wt rm {branch} -f",
    )


def delete_branch(branch: str, bare_path: Path, force: bool = False) -> None:
    """
    Delete the local git branch.

    Args:
        branch: Branch name to delete
        bare_path: Path to bare repository
        force: If True, use -D to force delete unmerged branches
    """
    # Use -D if force, otherwise -d
    delete_flag = "-D" if force else "-d"
    result = run_git("branch", delete_flag, branch, cwd=bare_path, check=False)

    if result.returncode == 0:
        if force:
            output.success(f"Force deleted unmerged branch: {branch}")
        else:
            output.success(f"Deleted branch: {branch}")
    elif "not found" not in result.stderr:
        # Some other error (but ignore "not found" since branch might already be deleted)
        error_msg = result.stderr.strip()

        if force:
            # Already using force, something else is wrong
            output.error_multiline(
                f"Failed to force delete branch: {error_msg}",
            )
        else:
            # Not using force, suggest it
            output.error_multiline(
                f"Failed to delete branch: {error_msg}",
                "",
                "The worktree has been removed, but the branch still exists.",
                "To complete the deletion, run:",
                f"  wt rm {branch} --force",
            )


def cleanup_and_delete_branch(
    worktree_path: Path, root: Path, branch: str, bare_path: Path, force: bool
) -> None:
    """
    Clean up empty parent directories and delete the branch.

    This helper consolidates the common pattern of cleaning up empty directories
    (e.g., for branches like release/202512) and then deleting the local branch.

    Args:
        worktree_path: Path to the worktree that was removed
        root: Root directory (for cleanup boundary)
        branch: Branch name to delete
        bare_path: Path to bare repository
        force: Whether to force delete the branch
    """
    # Clean up empty parent directories (for branches like release/202512)
    cleanup_empty_parent_dirs(worktree_path, root)

    # Delete the local branch
    delete_branch(branch, bare_path, force)


@argh.arg(
    "branch", nargs="?", help="Branch name to remove (infers from current directory if not provided)"
)
@argh.arg(
    "-f", "--force", help="Force removal of worktree with uncommitted changes and unmerged branches"
)
def main(branch: str | None, *, force: bool = False) -> None:
    """Remove a git worktree and its local branch.

    Examples:
      wt rm feature/old             # Remove worktree and branch (from any directory)
      wt rm                         # Remove current worktree and branch (when inside it)
      wt rm release/202512 -f       # Force remove with uncommitted changes/unmerged branch
      wt rm feature/old -f          # Complete partial deletion (if worktree already removed)
    """
    # Get branch name from argument or current worktree
    branch = resolve_branch_argument(branch)

    root = get_worktree_root_or_exit()
    bare_path = get_bare_path(root)

    # Try to resolve worktree path, but allow graceful recovery from partial deletions
    try:
        worktree_path = resolve_worktree_path(branch, must_exist=True)
    except SystemExit:
        # Worktree doesn't exist - might be a partial deletion
        # Be graceful and just try to delete the branch
        output.info(f"Worktree for '{branch}' not found, proceeding to delete branch only")
        delete_branch(branch, bare_path, force=force)
        ensure_readme()
        return

    # Check if branch can be deleted before removing worktree
    # This prevents orphaning the branch if deletion would fail
    check_branch_can_be_deleted(branch, bare_path, force)

    # Remove the worktree
    output.info(f"Removing worktree at {worktree_path}")
    try:
        # Build args functionally
        args = ["worktree", "remove", *(["--force"] if force else []), str(worktree_path)]

        run_git(*args, cwd=bare_path)
        output.success(f"Successfully removed worktree at {worktree_path}")

        cleanup_and_delete_branch(worktree_path, root, branch, bare_path, force)
        ensure_readme()
    except subprocess.CalledProcessError as e:
        # If the worktree is broken (missing .git file), manually clean up
        error_output = (e.stderr or "") + (e.stdout or "")
        if "does not exist" in error_output or "validation failed" in error_output:
            output.info("Worktree appears to be broken, attempting manual cleanup...")

            # Manually remove the directory if it exists
            if worktree_path.exists():
                try:
                    import shutil

                    shutil.rmtree(worktree_path)
                    output.info(f"Removed directory: {worktree_path}")
                except Exception as rm_error:
                    error_exit(f"Failed to remove directory: {rm_error}")

            # Prune stale worktree references
            try:
                run_git("worktree", "prune", cwd=bare_path, capture=False)
                output.info("Pruned stale worktree references")
                output.success(f"Successfully cleaned up worktree: {worktree_path}")

                cleanup_and_delete_branch(worktree_path, root, branch, bare_path, force)
                ensure_readme()
            except subprocess.CalledProcessError as prune_error:
                error_exit(f"Failed to prune worktree references: {prune_error}")
        else:
            # Show the actual git error message
            error_msg = e.stderr.strip() if e.stderr else str(e)
            error_exit(f"Failed to remove worktree: {error_msg}")


if __name__ == "__main__":
    argh.dispatch_command(main)
