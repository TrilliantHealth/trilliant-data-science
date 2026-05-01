"""
Generate completion candidates for shell completion.
Used by bash and zsh completion scripts.

Modes:
  --worktrees: Output worktree names (one per line)
  --branches: Output git branch names
  --subcommands: Output wt subcommands
  --with-descriptions: Include tab-separated descriptions

Examples:
  completion.py --worktrees
  completion.py --worktrees --with-descriptions
  completion.py --branches
  completion.py --subcommands
"""

from __future__ import annotations

import subprocess
import sys

import argh

# Import command definitions from main entry point
from thds.gent.__main__ import COMMANDS
from thds.gent.utils import find_worktree_root, get_bare_path, get_branch_list, parse_git_worktree_list


def get_worktrees(with_descriptions: bool = False) -> list[str]:
    """
    Get list of worktree names for completion.

    Args:
        with_descriptions: If True, include tab-separated descriptions

    Returns:
        List of worktree completion candidates
    """
    root = find_worktree_root()
    if root is None:
        return []

    try:
        worktrees = parse_git_worktree_list(root)
    except (subprocess.CalledProcessError, OSError, ValueError):
        return []

    # Build completion candidates, skipping bare repository
    return [
        (
            f"{str(wt.relative)}\t[{wt.branch or 'detached'}] {wt.head or 'unknown'}"
            if with_descriptions
            else str(wt.relative)
        )
        for wt in worktrees
        if not wt.bare
    ]


def get_branches() -> list[str]:
    """
    Get list of git branch names for completion.

    Returns:
        List of branch names (local and remote)
    """
    root = find_worktree_root()
    if root is None:
        return []

    bare_path = get_bare_path(root)
    if not bare_path.exists():
        return []

    try:
        # Get local branches
        local_branches = get_branch_list(cwd=bare_path, remote=False)

        # Get remote branches (with prefix stripped)
        remote_branches = get_branch_list(cwd=bare_path, remote=True, strip_remote_prefix=True)

        # Combine and remove duplicates
        all_branches = sorted(set(local_branches + remote_branches))
        return all_branches

    except (subprocess.CalledProcessError, OSError, ValueError):
        return []


def get_subcommands(with_descriptions: bool = False) -> list[str]:
    """
    Get list of wt subcommands for completion.

    Args:
        with_descriptions: If True, include tab-separated descriptions

    Returns:
        List of subcommand completion candidates
    """
    if with_descriptions:
        return [f"{cmd}\t{info['description']}" for cmd, info in COMMANDS.items()]
    else:
        return list(COMMANDS.keys())


@argh.arg("--worktrees", help="List worktree names")
@argh.arg("--branches", help="List git branch names")
@argh.arg("--subcommands", help="List wt subcommands")
@argh.arg("--with-descriptions", help="Include descriptions (tab-separated)")
def completion_command(
    worktrees: bool = False,
    branches: bool = False,
    subcommands: bool = False,
    with_descriptions: bool = False,
) -> None:
    """
    Generate completion candidates for shell completion.

    Must specify exactly one mode: --worktrees, --branches, or --subcommands.
    """
    # Count how many modes are specified
    modes = sum([worktrees, branches, subcommands])

    if modes == 0:
        print("Error: Must specify one mode: --worktrees, --branches, or --subcommands", file=sys.stderr)
        print(__doc__, file=sys.stderr)
        sys.exit(1)

    if modes > 1:
        print("Error: Can only specify one mode at a time", file=sys.stderr)
        sys.exit(1)

    # Generate completions based on mode
    if worktrees:
        candidates = get_worktrees(with_descriptions=with_descriptions)
    elif branches:
        candidates = get_branches()
        # Branches don't currently support descriptions
    elif subcommands:
        candidates = get_subcommands(with_descriptions=with_descriptions)
    else:
        candidates = []

    # Output candidates (one per line)
    for candidate in candidates:
        print(candidate)


def main() -> None:
    """Entry point for wt-completion command."""
    try:
        argh.dispatch_command(completion_command)
    except Exception:
        # Fail gracefully - important for shell completion
        # Don't print errors, just exit successfully with no output
        sys.exit(0)


if __name__ == "__main__":
    main()
