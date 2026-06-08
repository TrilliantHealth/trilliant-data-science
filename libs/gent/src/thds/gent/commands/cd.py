"""
Resolve the path to a worktree by branch name.
Used by the 'wt cd' command.
"""

import argh

from thds.gent.readme import ensure_readme
from thds.gent.shell_check import warn_if_no_shell_wrapper
from thds.gent.utils import error_exit, resolve_worktree_path


@argh.arg("branch", help="Branch name of the worktree to navigate to ('-' switches to the previous one)")
def main(branch: str) -> None:
    """Resolve the path to a worktree by branch name.

    Examples:
      wt cd main                # Navigate to main worktree
      wt cd feature/test        # Navigate to feature/test worktree
      wt cd -                   # Switch back to the previous worktree
    """
    if branch == "-":
        # The shell wrapper intercepts `-` to jump to the previous worktree. If we
        # reach Python with it, the wrapper isn't active and there's nothing to do.
        error_exit(
            "'wt cd -' (previous worktree) requires the wt shell integration; run 'wt setup-shell'"
        )

    worktree_path = resolve_worktree_path(branch, must_exist=True)
    print(worktree_path)
    warn_if_no_shell_wrapper()
    ensure_readme()


if __name__ == "__main__":
    argh.dispatch_command(main)
