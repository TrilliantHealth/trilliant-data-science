"""
Remove a worktree by moving it into a trash directory and deleting its files in the background.

A worktree full of virtualenvs can hold hundreds of thousands of small files, and
`git worktree remove` deletes them one at a time before returning. Renaming the
directory is instant, so git can forget the worktree right away; the deletion then
happens in a detached, low-priority process.
"""

import errno
import os
import subprocess
import sys
import traceback
from pathlib import Path
from uuid import uuid4

from thds.gent import empty_trash, output
from thds.gent.utils import run_git

# Lives inside the bare repo so that it is on the same volume as the worktrees,
# which is what makes the move a rename rather than a copy.
_TRASH_DIRNAME = "gent-trash"


def trash_dir(bare_path: Path) -> Path:
    return bare_path / _TRASH_DIRNAME


def is_locked(worktree_path: Path) -> bool:
    """Whether `git worktree lock` has been applied to this worktree."""
    dot_git = worktree_path / ".git"
    if not dot_git.is_file():
        return False

    gitdir = dot_git.read_text().strip().removeprefix("gitdir:").strip()
    return (worktree_path / gitdir / "locked").exists()


def move_to_trash(worktree_path: Path, bare_path: Path) -> bool:
    """Move the worktree into the trash and have git forget it.

    Returns False, having changed nothing, when the worktree is on a different
    volume from the bare repo; the caller should remove it the ordinary way.
    """
    trash = trash_dir(bare_path)
    trash.mkdir(exist_ok=True)
    try:
        worktree_path.rename(trash / f"{worktree_path.name}-{uuid4().hex[:8]}")
    except OSError as e:
        if e.errno != errno.EXDEV:
            raise
        output.warning(
            f"{worktree_path} is on a different volume from {bare_path}; removing it in place"
        )
        return False

    run_git("worktree", "prune", cwd=bare_path)
    return True


def _lower_own_priority() -> None:
    if sys.platform == "darwin":
        # background QoS: throttles disk I/O as well as CPU
        subprocess.run(["taskpolicy", "-b", "-p", str(os.getpid())], check=False)
    else:
        os.nice(19)


def _run_detached(bare_path: Path) -> None:
    os.setsid()
    os.chdir(bare_path)
    with open(os.devnull) as null, empty_trash.log_path(trash_dir(bare_path)).open("a") as log:
        os.dup2(null.fileno(), 0)
        os.dup2(log.fileno(), 1)
        os.dup2(log.fileno(), 2)
    _lower_own_priority()
    empty_trash.empty(trash_dir(bare_path))


def empty_in_background(bare_path: Path) -> None:
    """Fork a detached process that deletes everything in the trash, including
    anything an earlier one left behind. Returns immediately.

    A fork rather than a new interpreter, because the Python running gent may be
    installed in the worktree that was just moved into the trash.
    """
    if os.fork():
        return

    exit_code = 0
    try:
        _run_detached(bare_path)
    except BaseException:
        traceback.print_exc()
        exit_code = 1
    finally:
        sys.stderr.flush()
        os._exit(exit_code)
