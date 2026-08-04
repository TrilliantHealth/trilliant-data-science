"""
Report anything notable about a worktree, in one line.
Used by the 'wt status' command.

Silence means nothing is wrong. This is deliberate: a status line that always
prints the branch name teaches you to stop reading it, and the failure this
exists to catch - a worktree whose directory name no longer matches its HEAD -
is invisible in a branch name alone. It is only visible as a disagreement
between two facts, so only the disagreement is printed.
"""

import subprocess
import typing as ty
from pathlib import Path

import argh

from thds.gent import output
from thds.gent.utils import (
    WorktreeInfo,
    find_worktree_root,
    parse_git_worktree_list,
    run_git,
)


def _reportable(worktrees: ty.Iterable[WorktreeInfo]) -> list[WorktreeInfo]:
    """Real worktrees only.

    The bare repository is skipped. In a `wt clone` layout it is a `.bare`
    directory holding a sentinel branch rather than something git reports as
    bare, so the directory name is what identifies it.
    """
    return [wt for wt in worktrees if not wt.bare and wt.path.name != ".bare"]


def _by_name(worktrees: ty.Iterable[WorktreeInfo], target: str) -> WorktreeInfo | None:
    return next(
        (wt for wt in worktrees if wt.branch == target or str(wt.relative) == target),
        None,
    )


def _containing(worktrees: ty.Iterable[WorktreeInfo], directory: Path) -> WorktreeInfo | None:
    """The worktree *directory* lives in, which may be several levels down inside it."""
    resolved = directory.resolve()
    return max(
        (wt for wt in worktrees if resolved == wt.path or wt.path in resolved.parents),
        key=lambda wt: len(wt.path.parts),
        default=None,
    )


def _dirty_count(worktree: Path) -> int:
    result = run_git("status", "--porcelain", cwd=worktree, check=False)
    return len([line for line in result.stdout.splitlines() if line.strip()])


def _unpushed_commits(worktree: Path) -> int:
    """Commits on HEAD that the upstream does not have. Zero when there is no upstream."""
    result = run_git("rev-list", "--count", "@{upstream}..HEAD", cwd=worktree, check=False)
    if result.returncode != 0:
        return 0

    return int(result.stdout.strip() or 0)


def _unmerged_commits(worktree: Path, base: str) -> int | None:
    """Commits on HEAD not reachable from *base*, or None if base is unknown."""
    result = run_git("rev-list", "--count", f"{base}..HEAD", cwd=worktree, check=False)
    if result.returncode != 0:
        return None

    return int(result.stdout.strip() or 0)


def _mismatch(info: WorktreeInfo) -> str | None:
    """The gent convention is that a worktree's directory name IS its branch name."""
    if info.bare or info.detached:
        return None

    if info.branch and info.branch != str(info.relative):
        return f"dir!=HEAD (dir {info.relative}, HEAD {info.branch})"

    return None


def _status_parts(info: WorktreeInfo, base: str) -> list[str]:
    """Everything notable about *info*, as display strings. Empty means all clear.

    A missing upstream and drift behind upstream are both left out on purpose:
    an unpushed local branch is the normal state of a fresh worktree, and every
    worktree falls behind as main moves. Reporting either would mean most
    worktrees are never silent, which defeats the point.
    """
    dirty = _dirty_count(info.path)
    unpushed = _unpushed_commits(info.path)
    unmerged = _unmerged_commits(info.path, base)

    return [
        part
        for part in [
            _mismatch(info),
            "detached HEAD" if info.detached else None,
            f"{dirty} dirty" if dirty else None,
            f"{unpushed} unpushed" if unpushed else None,
            f"{unmerged} not in {base}" if unmerged else None,
        ]
        if part is not None
    ]


@argh.arg("target", nargs="?", help="Branch name or path (default: the current worktree)")
@argh.arg("--base", help="Base ref for the not-yet-merged count")
@argh.arg("--all", help="Report on every worktree in the repository")
def main(target: str | None, *, base: str = "origin/main", all: bool = False) -> None:
    """Print a one-line status for a worktree, or nothing if it is unremarkable.

    Intended for scripting and for status bars: stdout is one line per
    worktree, prefixed with the worktree name when reporting on more than one.

    Examples:
      wt status                     # the worktree you are standing in
      wt status feature/old         # a specific worktree
      wt status --all               # everything notable in the whole repo
    """
    root = find_worktree_root()
    if root is None:
        output.warning("Not inside a gent worktree repository")
        return

    try:
        worktrees = _reportable(parse_git_worktree_list(root))
    except (subprocess.CalledProcessError, OSError, ValueError) as e:
        output.warning(f"Failed to list worktrees: {e}")
        return

    # Plain print, not output.print_output: one line per worktree is a contract
    # for callers that parse this, and rich would wrap long lines.
    if all:
        for wt in worktrees:
            parts = _status_parts(wt, base)
            if parts:
                print(f"{wt.relative}: {', '.join(parts)}")
        return

    selected = _by_name(worktrees, target) if target else _containing(worktrees, Path.cwd())
    if selected is None:
        output.warning(f"No worktree found for {target or Path.cwd()}")
        return

    parts = _status_parts(selected, base)
    if parts:
        print(", ".join(parts))


if __name__ == "__main__":
    argh.dispatch_command(main)
