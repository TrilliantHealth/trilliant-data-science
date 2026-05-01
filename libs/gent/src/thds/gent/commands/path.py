"""
Print the fully qualified path of a worktree.
Used by the 'wt path' command.
"""

from __future__ import annotations

import argh

from thds.gent.readme import ensure_readme
from thds.gent.utils import resolve_branch_argument, resolve_worktree_path


@argh.arg(
    "branch",
    nargs="?",
    help="Branch name of the worktree (infers from current directory if not provided)",
)
def main(branch: str | None) -> None:
    """Print the fully qualified path of a worktree.

    Examples:
      wt path                   # Print path of current worktree
      wt path main              # Print path of main worktree
      wt path feature/test      # Print path of feature/test worktree
    """
    # Resolve branch argument - use current worktree if not provided
    branch = resolve_branch_argument(branch)
    worktree_path = resolve_worktree_path(branch, must_exist=True)
    print(worktree_path)
    ensure_readme()


if __name__ == "__main__":
    argh.dispatch_command(main)
