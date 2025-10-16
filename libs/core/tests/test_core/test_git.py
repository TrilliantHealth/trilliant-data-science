from pathlib import Path

from thds.core import git

REPO_ROOT = Path(__file__).parents[4]


def test_get_repo_root():
    for parent_dir in Path(__file__).resolve().parents:
        if (parent_dir / ".git").is_dir():
            assert git.get_repo_root() == parent_dir
            return
    raise EnvironmentError("We are mysteriously not in a .git project")


def test_git_commit_datetime_and_hash():
    """This test depends on the referenced file not being changed.

    If it does change, just change the values below.
    """
    dt, hash = git.get_commit_datetime_and_hash(
        "libs/core/tests/__init__.py", cwd=str(git.get_repo_root())
    )
    assert dt, hash == ("20221111.2001", "ac171a571f764fbd2522f254b1aa162220e7867c")
