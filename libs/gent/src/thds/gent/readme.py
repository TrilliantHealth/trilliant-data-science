"""
Generate a README.md at the bare repo root.

The README provides context for LLMs and developers about the worktree
structure, available commands, and links to documentation. It is created
once and refreshed if its timestamp is older than one day.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path

from thds.gent._repo import GENT_README, GENT_REPO_PATH, github_tree_url

from thds.gent.utils import find_worktree_root

logger = logging.getLogger(__name__)

MANAGED_MARKER = "<!-- This file is managed by gent (wt). -->"

_ONE_DAY_SECONDS = 86400


def generate_readme_content(repo_name: str, default_branch: str = "main") -> str:
    """Generate the README markdown content. Pure function.

    Args:
        repo_name: Name of the repository (directory name).
        default_branch: Primary worktree name for documentation links.
    """
    base_url = github_tree_url(branch=default_branch)
    docs_extra = ""
    gent_readme = f"{base_url}/{GENT_REPO_PATH}/{GENT_README}"
    gent_link = f"[wt (gent)]({gent_readme})"
    docs_section = f"{docs_extra}- **wt (gent) full reference:** [README]({gent_readme})"
    return f"""{MANAGED_MARKER}

# {repo_name}

This repository uses a **bare repo + worktree** structure managed by
**{gent_link}**.

## For LLMs / AI Agents

**IMPORTANT:** In this repository, `wt` (gent) is the **required** tool for
managing worktrees. You MUST use `wt` commands — do NOT use raw
`git worktree`, `git checkout`, or `git branch` commands directly.

Branches should NOT be created independently of worktrees. Every branch
corresponds to a worktree directory whose path matches the branch name.

### Essential Commands

| Command                 | Description                                |
| ----------------------- | ------------------------------------------ |
| `wt co <branch> [base]` | Create or checkout a branch as a worktree  |
| `wt rm <branch>`        | Remove a worktree and delete the branch    |
| `wt list`               | List all worktrees                         |
| `wt cd <branch>`        | Navigate to a worktree                     |
| `wt path <branch>`      | Print worktree path (for scripting)        |
| `wt init [branch]`      | Run worktree initialization script         |
| `wt start [branch]`     | Run start hook (e.g. open editor)          |

### Listing Current Worktrees

Run `wt list` to see all active worktrees and their branches.

## Documentation

{docs_section}

## Repository Layout

```
{repo_name}/
├── .bare/              # Git metadata (bare repository)
├── {default_branch}/              # Default branch worktree
├── feature/*/          # Feature branch worktrees
└── README.md           # This file (managed by gent)
```
"""


def _is_stale(path: Path) -> bool:
    """Return True if *path* was last modified more than one day ago."""
    return (time.time() - path.stat().st_mtime) > _ONE_DAY_SECONDS


def ensure_readme(root: Path | None = None, default_branch: str = "main") -> None:
    """Create or refresh a README.md at the bare repo root.

    Finds the worktree root automatically when *root* is not provided.
    The file is written when it does not exist or when its modification
    timestamp is older than one day.

    Safe to call from any context — catches all exceptions to ensure
    README generation never breaks a real wt command.
    """
    try:
        if root is None:
            root = find_worktree_root()
        if root is None:
            return
        readme_path = root / "README.md"
        if readme_path.exists() and not _is_stale(readme_path):
            return
        repo_name = root.resolve().name
        content = generate_readme_content(repo_name, default_branch)
        readme_path.write_text(content)
    except Exception:
        logger.debug("Failed to create/update README.md at %s", root, exc_info=True)
