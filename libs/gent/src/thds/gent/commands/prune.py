"""
Prune worktrees whose remote branch no longer exists.
Used by the 'wt prune' command.
"""

import subprocess
import time
import typing as ty
from pathlib import Path

import argh

from thds.gent import output
from thds.gent.commands.rm import cleanup_and_delete_branch
from thds.gent.readme import ensure_readme
from thds.gent.utils import (
    WorktreeInfo,
    dirty_count,
    extract_subprocess_error,
    get_bare_path,
    get_branch_list,
    get_worktree_root_or_exit,
    parse_git_worktree_list,
    repair_fetch_refspec,
    run_git,
    unmerged_commits,
)

Candidate = tuple[WorktreeInfo, int, int | None]

_MIN_AGE_DAYS = 3


def _get_default_branch(bare_path: Path) -> str | None:
    """Read the default branch from the bare repo's HEAD."""
    try:
        result = run_git("symbolic-ref", "--short", "HEAD", cwd=bare_path)
        return result.stdout.strip() or None
    except subprocess.CalledProcessError:
        return None


def _is_prunable(wt: WorktreeInfo, remote_branches: frozenset[str], default_branch: str | None) -> bool:
    """Whether a worktree is a candidate for pruning."""
    if wt.bare or wt.detached or not wt.branch:
        return False
    if not wt.path.is_dir():
        return False
    if (time.time() - wt.path.stat().st_mtime) / 86400 < _MIN_AGE_DAYS:
        return False
    if wt.branch == default_branch:
        return False
    return wt.branch not in remote_branches


def _needs_force(dirty: int, unmerged: int | None) -> bool:
    return dirty > 0 or (unmerged is None or unmerged > 0)


def _find_candidates(
    worktrees: ty.Sequence[WorktreeInfo],
    remote_branches: frozenset[str],
    default_branch: str | None,
    base_ref: str,
) -> list[Candidate]:
    """Identify worktrees whose branch has no remote counterpart."""
    return [
        (wt, dirty_count(wt.path), unmerged_commits(wt.path, base_ref))
        for wt in worktrees
        if _is_prunable(wt, remote_branches, default_branch)
    ]


def _escape_markup(text: str) -> str:
    """Escape Rich markup brackets in text so they display literally."""
    return text.replace("[", "\\[")


def _print_candidates(candidates: ty.Sequence[Candidate], force: bool) -> None:
    """Print the numbered list of worktrees that would be pruned."""
    output.info(f"Found {len(candidates)} worktree(s) with no remote branch:\n")
    for i, (wt, dirty, unmerged) in enumerate(candidates, 1):
        branch_display = _escape_markup(wt.branch or "")
        parts: list[str] = []
        if unmerged is None:
            parts.append("unmerged: unknown")
        elif unmerged > 0:
            parts.append(f"{unmerged} not in main")
        if dirty > 0:
            parts.append(f"dirty: {dirty} uncommitted")
        if parts:
            status_text = ", ".join(parts)
            if not force:
                status_text += " -- needs --force"
            status = f"  [yellow]({status_text})[/yellow]"
        else:
            status = ""
        output.print_output(f"  {i}. {branch_display}{status}")
    print()


def _select_candidates(candidates: list[Candidate], yes: bool, force: bool) -> list[Candidate]:
    """Prompt the user to select which worktrees to remove."""
    if yes:
        if force:
            return candidates
        safe = [c for c in candidates if not _needs_force(c[1], c[2])]
        skipped = len(candidates) - len(safe)
        if skipped:
            output.warning(
                f"Skipped {skipped} worktree(s) with unmerged commits"
                " or uncommitted changes (use --force)"
            )
        return safe

    try:
        response = input("Remove which worktrees? (all/none/1,2,...) [none] ").strip().lower()
    except (EOFError, KeyboardInterrupt):
        print()
        return []

    if not response or response in ("none", "n"):
        return []

    if response in ("all", "a", "y", "yes"):
        if not force:
            safe = [c for c in candidates if not _needs_force(c[1], c[2])]
            skipped = len(candidates) - len(safe)
            if skipped:
                output.warning(
                    f"Skipped {skipped} worktree(s) with unmerged commits"
                    " or uncommitted changes (use --force)"
                )
            return safe
        return candidates

    selected: list[Candidate] = []
    seen: set[int] = set()
    for part in response.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            num = int(part)
        except ValueError:
            output.warning(f"Invalid selection: {part!r}")
            return []
        if num < 1 or num > len(candidates):
            output.warning(f"Out of range: {num} (expected 1-{len(candidates)})")
            return []
        if num not in seen:
            seen.add(num)
            selected.append(candidates[num - 1])

    if not force:
        blocked = [c for c in selected if _needs_force(c[1], c[2])]
        if blocked:
            names = ", ".join(c[0].branch or "" for c in blocked)
            output.warning(f"Cannot remove without --force: {names}")
            return []

    return selected


def _remove_worktree(
    wt: WorktreeInfo, root: Path, bare_path: Path, *, force_remove: bool, force_delete: bool
) -> None:
    """Remove a single worktree and its branch."""
    branch = wt.branch or ""
    output.info(f"Removing worktree: {branch}")

    args = ["worktree", "remove", *(["--force"] if force_remove else []), str(wt.path)]
    try:
        run_git(*args, cwd=bare_path)
    except subprocess.CalledProcessError as e:
        error_msg = extract_subprocess_error(e)
        output.warning(f"Failed to remove worktree {branch}: {error_msg}")
        return

    output.success(f"Removed worktree: {branch}")
    # Catch SystemExit because delete_branch calls error_multiline (which
    # calls sys.exit) on unexpected failures — that must not abort the loop.
    try:
        cleanup_and_delete_branch(wt.path, root, branch, bare_path, force=force_delete)
    except SystemExit:
        pass


@argh.arg("-y", "--yes", help="Skip prompt; removes only clean, merged worktrees unless --force")
@argh.arg("-f", "--force", help="Include worktrees with unmerged commits or uncommitted changes")
def main(*, yes: bool = False, force: bool = False) -> None:
    """Prune worktrees whose remote branch has been deleted.

    Fetches with --prune to sync remote refs, then finds worktrees whose
    branch no longer exists on the remote. Candidates with unmerged commits
    or uncommitted changes need --force to be removed.

    Examples:
      wt prune                       # Interactive: select which to remove
      wt prune --yes                 # Remove only clean, merged worktrees
      wt prune --yes --force         # Remove all without prompting
    """
    root = get_worktree_root_or_exit()
    bare_path = get_bare_path(root)

    repair_fetch_refspec(bare_path)

    output.info("Fetching and pruning remote refs...")
    try:
        run_git("fetch", "--prune", cwd=bare_path)
    except subprocess.CalledProcessError as e:
        error_msg = extract_subprocess_error(e)
        output.warning(f"Fetch failed ({error_msg}), continuing with local state")

    worktrees = parse_git_worktree_list(root)
    try:
        remote_branches = frozenset(get_branch_list(bare_path, remote=True))
    except subprocess.CalledProcessError:
        output.warning("No remote branches found (is a remote configured?)")
        remote_branches = frozenset()

    default_branch = _get_default_branch(bare_path)

    if not default_branch:
        output.error_multiline(
            "Cannot determine the default branch (bare repo HEAD is detached or missing).",
            "Cannot safely identify stale worktrees.",
        )

    if default_branch not in remote_branches:
        output.error_multiline(
            f"Default branch '{default_branch}' has no remote tracking branch.",
            "Cannot safely identify stale worktrees without a trusted remote view.",
            "Ensure a remote is configured and reachable, then try again.",
        )

    base_ref = f"origin/{default_branch}"
    candidates = _find_candidates(worktrees, remote_branches, default_branch, base_ref)

    if not candidates:
        output.success("Nothing to prune -- all worktree branches exist on the remote.")
        return

    _print_candidates(candidates, force)

    selected = _select_candidates(candidates, yes, force)
    if not selected:
        if not yes:
            output.info("Aborted.")
        return

    for wt, dirty, unmerged in selected:
        _remove_worktree(
            wt,
            root,
            bare_path,
            force_remove=dirty > 0,
            force_delete=unmerged is None or unmerged > 0,
        )

    ensure_readme()
    output.success("Done.")


if __name__ == "__main__":
    argh.dispatch_command(main)
