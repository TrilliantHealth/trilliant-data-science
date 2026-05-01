"""
Run the repository's .gent/start script (e.g. open an editor).
Used by the 'wt start' command.
"""

from __future__ import annotations

import argh

from thds.gent import output
from thds.gent.hooks import hook_exists, run_hook
from thds.gent.utils import resolve_branch_argument, resolve_worktree_path


@argh.arg(
    "branch", nargs="?", help="Branch name to start (infers from current directory if not provided)"
)
def main(branch: str | None) -> None:
    """Open a worktree for active development (e.g. launch editor).

    Searches for start scripts in this order:
        1. <worktree>/.gent/start.local (user customization, worktree-specific)
        2. <bare-root>/.gent/start.local (user customization, repo-specific)
        3. <worktree>/.gent/start (shared/team default)

    Runs the first found script with environment variables:
        GENT_WORKTREE_PATH: Absolute path to the worktree
        GENT_BRANCH: Branch name

    Examples:
      wt start                  # Start working in current worktree
      wt start feature/new      # Start working in specific worktree
    """
    branch = resolve_branch_argument(branch)
    worktree_path = resolve_worktree_path(branch, must_exist=True)

    if not hook_exists("start", worktree_path):
        output.error(
            "No start script found.\n\n"
            "To set up a start action, create an executable .gent/start script in your repository.\n"
            "See the gent documentation for examples."
        )

    if not run_hook("start", worktree_path, branch):
        output.error("Start script failed")

    output.success(f"Started worktree: {worktree_path}")


if __name__ == "__main__":
    argh.dispatch_command(main)
