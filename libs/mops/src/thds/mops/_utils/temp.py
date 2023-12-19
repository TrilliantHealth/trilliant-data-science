"""Utility to make returning files via their Paths less confusing in the application code."""
import tempfile
from pathlib import Path
from threading import Lock
from typing import Optional

from thds.core import log

logger = log.getLogger(__name__)


class TempDirPath:
    """Lazily creates a temporary directory and returns it as a Path.

    The temporary directory will not get cleaned up until the interpreter exits or cleanup is called.

    This is a common use case not just for our internal code but also
    for users of `mops` if they want to create a Path for
    return from a remote process.
    """

    def __init__(self):
        self.lock = Lock()
        self.tempdir: Optional[tempfile.TemporaryDirectory] = None

    def __call__(self) -> Path:
        with self.lock:
            if self.tempdir is None:
                self.tempdir = tempfile.TemporaryDirectory(prefix="mops-tmp-")
            return Path(self.tempdir.name)

    def cleanup(self):
        """Don't call this unless you know what you're doing.

        Even if you never call this, the encapsulated tempdir should
        automatically get cleaned up at interpreter exit.
        """
        with self.lock:
            if self.tempdir:
                logger.info(f"Cleaning up tempdir {self.tempdir.name}")
                self.tempdir.cleanup()
                self.tempdir = None


_REMOTE_TMP = TempDirPath()
# there's really no obvious reason why you'd ever need more than one
# of these, so we create one as a global for general use.


def tempdir() -> Path:
    """Lazily creates a global temporary directory and returns it as a Path.

    If you are running remotely, these files will get explicitly
    cleaned up after the main handler has run.

    If using this locally, the files won't get cleaned up until the interpreter exits.
    """
    return _REMOTE_TMP()
