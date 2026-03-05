from pathlib import Path

from thds.core import git


def test_get_repo_root():
    for parent_dir in Path(__file__).resolve().parents:
        if (parent_dir / ".git").exists():
            # .git can be a file (ex multiple worktrees), so just check for existence
            assert git.get_repo_root() == parent_dir
            return
    raise EnvironmentError("We are mysteriously not in a .git project")


def test_git_commit_datetime_and_hash():
    # no file pattern — just gets HEAD's datetime and hash, avoiding pathspec-limited
    # git log which is extremely slow in treeless CI clones (--filter=tree:0).
    dt, hash = git.get_commit_datetime_and_hash()
    yyyymmdd, hhmm = dt.split(".")
    assert len(yyyymmdd) == 8
    assert yyyymmdd.isdigit()
    assert yyyymmdd.startswith("20")
    assert len(hhmm) == 4
    assert hhmm.isdigit()
    assert int(hhmm[:2]) < 24
    assert int(hhmm[2:]) < 60
    assert len(hash) == 40
    assert all(c in "0123456789abcdef" for c in hash)
