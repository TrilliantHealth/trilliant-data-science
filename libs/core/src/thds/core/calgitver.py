"""Uses local git repo info to construct a more informative CalVer version string.

    This time format was chosen to be CalVer-esque but to drop time
    fractions smaller than minutes since they're exceeding rarely
    semantically meaningful, and the git commit hash will in 99.999%
    of cases be a great disambiguator for cases where multiple
    versions happen to be generated within the same minute by
    different users.

    We use only dots as separators to be compatible with both Container Registry
    formats and PEP440.
"""

import os
import re
from functools import lru_cache

from . import git

SHORT_HASH = 7


def uncached() -> str:
    """This is the 'proper', deterministic CalGitVer - unlike the nondeterministic
    meta.make_calgitver when the repo is dirty. It does allow for the possibility of
    override via environment variable, which is intended to support nonlocal runtime
    environments.

    Suitable for use any time you may be wanting to get this in a context where you're
    not sure that the git repo is present, but you expect the environment variable has
    been set if it isn't.

    In other words, prefer calling this one instead of meta.make_calgitver if you are
    trying to use this for production use cases, especially if in a Docker image or Spark
    cluster.

    """
    env_var = os.getenv("CALGITVER")
    if env_var:
        return env_var

    commit_datetime, commit_hash = git.get_commit_datetime_and_hash()
    return "-".join(
        filter(
            None,
            (
                commit_datetime,
                commit_hash[:SHORT_HASH],
                "" if git.is_clean() else "dirty",
            ),
        )
    )


cached = lru_cache(maxsize=1)(uncached)
calgitver = cached


def is_clean(cgv: str) -> bool:
    return bool(cgv and not cgv.endswith("-dirty"))


def clean_calgitver() -> str:
    """Only allow CalGitVer computed from a clean repository.

    Particularly useful for strict production environments.
    """
    cgv = calgitver()
    if not is_clean(cgv):
        raise ValueError(f"CalGitVer {cgv} was computed from a dirty repository!")
    return cgv


CALGITVER_EXTRACT_RE = re.compile(
    r"""
    (?P<year>\d{4})
    (?P<month>\d{2})
    (?P<day>\d{2})
    \.
    (?P<hour>\d{2})
    (?P<minute>\d{2})
    -
    (?P<git_commit>[a-f0-9]{7})
    (?P<dirty>(-dirty$)|$)
    """,
    re.X,
)


def parse_calgitver(maybe_calgitver: str):
    return CALGITVER_EXTRACT_RE.match(maybe_calgitver)
