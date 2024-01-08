# some basic git utilities.
#
# All of these will error if git is not available, or if the repo is not present.  The
# caller is expected to catch subprocess.CalledProcessError as well as FileNotFoundError.
import os
import subprocess as sp
import typing as ty

from . import log

LOGGER = log.getLogger(__name__)
CALGITVER_NO_SECONDS_FORMAT = "%Y%m%d.%H%M"


NO_GIT = (sp.CalledProcessError, FileNotFoundError)
# FileNotFoundError can happen if git is not installed at all.


def _simple_run(s_or_l_cmd: ty.Union[str, ty.List[str]], env=None, cwd=None) -> str:
    kwargs = dict(text=True, shell=True, env=env, cwd=cwd)
    if isinstance(s_or_l_cmd, list):
        kwargs["shell"] = False
    return sp.check_output(s_or_l_cmd, **kwargs).rstrip("\n")


def get_repo_name() -> str:
    return _simple_run("git remote get-url origin").split("/")[-1].rstrip().split(".")[0]


def get_commit_hash() -> str:
    LOGGER.debug("`get_commit` reading from Git repo.")
    return _simple_run("git rev-parse --verify HEAD")


def is_clean() -> bool:
    LOGGER.debug("`is_clean` reading from Git repo.")
    # command will print an empty string if the repo is clean
    return "" == _simple_run("git diff --name-status")


def get_branch() -> str:
    LOGGER.debug("`get_branch` reading from Git repo.")
    return _simple_run("git branch --show-current")


def get_commit_datetime_and_hash(
    *file_patterns: str,
    cwd: ty.Optional[str] = None,
    date_format: str = CALGITVER_NO_SECONDS_FORMAT,
) -> ty.Tuple[str, str]:
    """Useful for making a CalGitVer from a file or set of matching files.

    If no file patterns were provided, it will return the commit datetime and hash of the
    most recent commit.
    """
    assert " " not in date_format, "date_format cannot contain spaces"
    dt, hash = (
        _simple_run(
            # the space between %cd and %h allows us to split on it
            f"git log -n 1 --date=format-local:{date_format} --format=format:'%cd %H' -- "
            + " ".join(file_patterns),
            env=dict(os.environ, TZ="UTC0"),
            cwd=cwd,
        )
        .strip("'")
        .split(" ")
    )
    return dt, hash
