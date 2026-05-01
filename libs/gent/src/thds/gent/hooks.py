"""
Find and run .gent hook scripts (init, start, etc.).

Hook scripts are searched in precedence order:
    1. <worktree>/.gent/<hook>.local (user, worktree-specific)
    2. <bare-root>/.gent/<hook>.local (user, repo-specific)
    3. <worktree>/.gent/<hook> (shared/team default)
"""

import os
import subprocess
from pathlib import Path

from thds.gent import output
from thds.gent.utils import find_worktree_root


def _find_hook(hook_name: str, worktree_path: Path) -> Path | None:
    """Find a hook script by name, following precedence rules.

    Returns the first executable script found, or None.
    """
    bare_root = find_worktree_root(worktree_path)
    candidates = _candidates(hook_name, worktree_path, bare_root)

    for script in candidates:
        if script.exists() and os.access(script, os.X_OK):
            return script

    return None


def _candidates(hook_name: str, worktree_path: Path, bare_root: Path | None) -> list[Path]:
    return [
        worktree_path / ".gent" / f"{hook_name}.local",
        *([] if bare_root is None else [bare_root / ".gent" / f"{hook_name}.local"]),
        worktree_path / ".gent" / hook_name,
    ]


def _warn_not_executable(hook_name: str, worktree_path: Path) -> None:
    bare_root = find_worktree_root(worktree_path)
    for script in _candidates(hook_name, worktree_path, bare_root):
        if script.exists() and not os.access(script, os.X_OK):
            output.warning(
                f"{hook_name} script is not executable: {script}\n"
                f"Make it executable with: chmod +x {script}"
            )


def run_hook(hook_name: str, worktree_path: Path, branch: str) -> bool:
    """Find and run a .gent hook script.

    Returns True on success, False if the script failed or wasn't found.
    """
    script = _find_hook(hook_name, worktree_path)

    if script is None:
        _warn_not_executable(hook_name, worktree_path)
        return False

    output.info(f"Running {hook_name} script: {script.resolve()}")

    env = os.environ.copy()
    env.update(
        {
            "GENT_WORKTREE_PATH": str(worktree_path.resolve()),
            "GENT_BRANCH": branch,
        }
    )

    try:
        result = subprocess.run([str(script)], cwd=worktree_path, env=env, check=False)
    except (OSError, subprocess.TimeoutExpired) as e:
        output.warning(f"Failed to run {hook_name} script: {e}")
        return False

    if result.returncode != 0:
        output.warning(f"{hook_name} script failed with exit code {result.returncode}")
        return False

    return True


def hook_exists(hook_name: str, worktree_path: Path) -> bool:
    """Check whether a hook script exists (executable or not)."""
    bare_root = find_worktree_root(worktree_path)
    return any(script.exists() for script in _candidates(hook_name, worktree_path, bare_root))
