"""
Find and print the worktree root directory.
Used by the 'wt root' command.
"""

from thds.gent.readme import ensure_readme
from thds.gent.shell_check import warn_if_no_shell_wrapper
from thds.gent.utils import error_exit, find_worktree_root


def main() -> None:
    """Print the worktree root directory."""
    root = find_worktree_root()
    if root is None:
        error_exit("Not in a worktree directory (no .bare directory found)")

    print(root)
    warn_if_no_shell_wrapper()
    ensure_readme()


if __name__ == "__main__":
    main()
