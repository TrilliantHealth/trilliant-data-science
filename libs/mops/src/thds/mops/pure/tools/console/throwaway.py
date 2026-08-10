"""Telling a run nobody will look for from one somebody will.

Automated suites produce runs continuously, at a rate no person reads, and a library used
inside someone else's tests produces them in that project's own working directory. Either
way they crowd out the runs a person went looking for, which is the whole job of the place
they share.

Both halves of a run are moved aside together - the orchestrator's local directory and the
blob prefix its remotes report to - so a throwaway run is still a whole run, readable by
pointing the console at it.

Redirected rather than suppressed: a suite that writes nowhere stops covering the write
path, and a break in it would surface first in someone's real run.
"""

import os

from thds.core import config

THROWAWAY_RUNS = config.item("thds.mops.console.throwaway_runs", default=False, parse=config.tobool)
# forces the throwaway location whatever the environment says. Detection covers the usual
# cases; this is for a caller that knows its runs are disposable and is not running under
# anything that would be recognised.

_ENV_VARS = ("PYTEST_VERSION", "PYTEST_CURRENT_TEST", "CI")
# `PYTEST_VERSION` covers a whole session but only exists from pytest 8;
# `PYTEST_CURRENT_TEST` goes back further but is set only while a test is running, not
# during collection or session-scoped fixtures. Together they cover both. `CI` is set by
# every widely used automation host, and catches whatever the other two miss.


def here() -> bool:
    """Whether this process's runs are disposable."""
    return THROWAWAY_RUNS() or any(os.getenv(name) for name in _ENV_VARS)


def suffixed(name: str) -> str:
    """`name` for a real run, marked for a throwaway one.

    A suffix rather than a separate parent so the two sit side by side wherever runs are
    kept, and one `ls` shows both.
    """
    return f"{name}-throwaway" if here() else name
