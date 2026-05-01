"""
List all git worktrees.
Used by the 'wt list' command.
"""

import subprocess

from thds.gent import output
from thds.gent.readme import ensure_readme
from thds.gent.utils import error_exit, get_worktree_root_or_exit, parse_git_worktree_list


def main() -> None:
    """List all worktrees with their status."""
    root = get_worktree_root_or_exit()

    # Parse git worktree list
    try:
        worktrees = parse_git_worktree_list(root)
    except (subprocess.CalledProcessError, OSError, ValueError) as e:
        error_exit(f"Failed to list worktrees: {e}")

    # Print header
    output.print_output(f"Worktrees in {root}:\n")

    # Find the longest relative path for alignment
    max_len = max(len(str(wt.relative)) for wt in worktrees)

    # Print each worktree
    for wt in worktrees:
        relative = str(wt.relative)
        padding = " " * (max_len - len(relative) + 2)

        # Build status string (functional - no mutation)
        status_parts = tuple(
            part
            for part in [
                "(bare)" if wt.bare else None,
                wt.head if not wt.bare and wt.head else None,
                f"[{wt.branch}]" if not wt.bare and wt.branch else None,
                "[detached]" if not wt.bare and wt.detached and not wt.branch else None,
            ]
            if part is not None
        )

        status = " ".join(status_parts)
        output.print_output(f"  {relative}{padding}{status}")

    ensure_readme()


if __name__ == "__main__":
    main()
