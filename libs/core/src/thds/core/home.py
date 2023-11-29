import os
from pathlib import Path

from .config import item

# On our GitHub runners, we can't make hardlinks from /runner/home to where our stuff actually goes.
# This allows us to use 'a' home directory that is on the same filesystem.
_RUNNER_WORK = Path("/runner/_work")
if os.getenv("CI") and _RUNNER_WORK.exists() and _RUNNER_WORK.is_dir():
    __home = _RUNNER_WORK
else:
    __home = Path.home()


HOMEDIR = item("thds.core.homedir", parse=Path, default=__home)
