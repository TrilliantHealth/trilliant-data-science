"""
Resolve the path to a worktree by branch name.
Used by the 'wt cd' command.
"""

import argh

from thds.gent.readme import ensure_readme
from thds.gent.shell_check import warn_if_no_shell_wrapper
from thds.gent.utils import resolve_worktree_path


@argh.arg("branch", help="Branch name of the worktree to navigate to")
def main(branch: str) -> None:
    """Resolve the path to a worktree by branch name.

    Examples:
      wt cd main                # Navigate to main worktree
      wt cd feature/test        # Navigate to feature/test worktree
    """
    worktree_path = resolve_worktree_path(branch, must_exist=True)
    print(worktree_path)
    warn_if_no_shell_wrapper()
    ensure_readme()


if __name__ == "__main__":
    argh.dispatch_command(main)
