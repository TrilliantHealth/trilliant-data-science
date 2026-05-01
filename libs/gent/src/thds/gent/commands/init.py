"""
Initialize a worktree using the repository's .gent/init script.
Used by the 'wt init' command.
"""

from __future__ import annotations

import argh

from thds.gent import output
from thds.gent.hooks import hook_exists, run_hook
from thds.gent.readme import ensure_readme
from thds.gent.utils import resolve_branch_argument, resolve_worktree_path


@argh.arg(
    "branch", nargs="?", help="Branch name to initialize (infers from current directory if not provided)"
)
def main(branch: str | None) -> None:
    """Initialize a worktree using the repository's .gent/init script.

    Searches for init scripts in this order:
        1. <worktree>/.gent/init.local (user customization, worktree-specific)
        2. <bare-root>/.gent/init.local (user customization, repo-specific)
        3. <worktree>/.gent/init (shared/team default)

    Runs the first found script with environment variables:
        GENT_WORKTREE_PATH: Absolute path to the worktree
        GENT_BRANCH: Branch name

    Examples:
      wt init                   # Initialize current worktree
      wt init feature/new       # Initialize specific worktree
    """
    branch = resolve_branch_argument(branch)
    worktree_path = resolve_worktree_path(branch, must_exist=True)

    output.info(f"Initializing worktree at {worktree_path}")

    if not hook_exists("init", worktree_path):
        output.error(
            "No initialization script found.\n\n"
            "To set up initialization, create an executable .gent/init script in your repository.\n"
            "See the gent documentation for examples."
        )

    if not run_hook("init", worktree_path, branch):
        output.error("Initialization failed")

    output.success(f"Successfully initialized worktree: {worktree_path}")
    ensure_readme()


if __name__ == "__main__":
    argh.dispatch_command(main)
