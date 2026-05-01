"""
Clone a repository as a bare repo with worktrees.
Used by the 'wt clone' command.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import argh

from thds.gent import output
from thds.gent.readme import ensure_readme
from thds.gent.utils import error_exit, run_git, run_git_streaming


def determine_default_branch(bare_dir: Path) -> str:
    """
    Determine the default branch of a cloned repository.

    Tries multiple strategies in order:
    1. Check refs/remotes/origin/HEAD (if set)
    2. Query remote with ls-remote --symref
    3. Fallback to "main"

    Args:
        bare_dir: Path to the bare repository

    Returns:
        Name of the default branch
    """
    # First try to get it from symbolic-ref
    result = run_git("symbolic-ref", "refs/remotes/origin/HEAD", "--short", cwd=bare_dir, check=False)

    if result.returncode == 0 and result.stdout.strip():
        default_branch = result.stdout.strip()
        # Remove 'origin/' prefix
        if default_branch.startswith("origin/"):
            return default_branch[7:]
        return default_branch

    # Fallback: try to determine from ls-remote
    result = run_git("ls-remote", "--symref", "origin", "HEAD", cwd=bare_dir, check=False)

    if result.returncode == 0:
        # Parse output like: "ref: refs/heads/main  HEAD"
        for line in result.stdout.splitlines():
            if line.startswith("ref:"):
                return line.split("/")[-1].split()[0]

    # Final fallback: use "main"
    return "main"


@argh.arg("url", help="Git repository URL to clone")
@argh.arg("-d", "--directory", help="Directory name (defaults to repository name)")
@argh.arg("-b", "--branch", help="Branch to checkout (defaults to repository default branch)")
def main(url: str, *, directory: str | None = None, branch: str | None = None) -> None:
    """Clone a repository with bare repository + worktree structure.

    This is the easiest way to set up a new repository for use with wt.
    The repository is cloned as a bare repository with the main branch
    (or specified branch) checked out as a worktree.

    Examples:
      wt clone git@github.com:user/repo.git
      wt clone git@github.com:user/repo.git --directory my-repo
      wt clone https://github.com/user/repo.git --branch develop
    """
    # Determine directory name from URL if not provided
    if directory is None:
        # Extract repo name from URL
        # Examples:
        #   git@github.com:user/repo.git -> repo
        #   https://github.com/user/repo.git -> repo
        #   https://github.com/user/repo -> repo
        directory = url.rstrip("/").split("/")[-1]
        if directory.endswith(".git"):
            directory = directory[:-4]

    target_dir = Path.cwd() / directory
    bare_dir = target_dir / ".bare"

    # Check if directory already exists
    if target_dir.exists():
        error_exit(
            f"Directory already exists: {target_dir}\n\n"
            f"Choose a different name or remove the existing directory first."
        )

    output.info(f"Cloning {url}...")
    output.info(f"Target directory: {target_dir}")

    try:
        # Create target directory
        target_dir.mkdir(parents=True)

        # Clone as bare repository with streaming output
        output.info("Creating bare repository...")
        run_git_streaming("clone", "--bare", url, str(bare_dir))
        run_git_streaming(
            "--git-dir=.bare",
            "config",
            "remote.origin.fetch",
            "'+refs/heads/*:refs/remotes/origin/*'",
            cwd=target_dir,
        )  # Enable fetching remote branches
        output.success(f"Cloned bare repository to {bare_dir}")

        # Determine branch to checkout
        if branch is None:
            output.info("Determining default branch...")
            branch = determine_default_branch(bare_dir)
            output.info(f"Using default branch: {branch}")
        else:
            output.info(f"Using specified branch: {branch}")

        # Create main worktree with streaming output
        output.info(f"Creating worktree for {branch}...")
        worktree_path = target_dir / branch
        run_git_streaming("worktree", "add", str(worktree_path), branch, cwd=bare_dir)
        output.success(f"Created worktree: {worktree_path}")
        ensure_readme(target_dir, default_branch=branch)
        output.success("\n✓ Successfully cloned repository!")
        output.info(f"\nTo start working:\n  cd {target_dir}/{branch}")

    except subprocess.CalledProcessError as e:
        # Clean up on failure
        if target_dir.exists():
            import shutil

            shutil.rmtree(target_dir)

        error_msg = e.stderr.strip() if e.stderr else str(e)
        error_exit(f"Clone failed: {error_msg}")
    except Exception as e:
        # Clean up on failure
        if target_dir.exists():
            import shutil

            shutil.rmtree(target_dir)

        error_exit(f"Clone failed: {e}")


if __name__ == "__main__":
    argh.dispatch_command(main)
