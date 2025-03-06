import random
import time
from datetime import timedelta
from pathlib import Path

from filelock import FileLock

from thds.core import config, home, log

from .md5 import hex_md5_str

DOWNLOAD_LOCKS_DIR = config.item("dir", home.HOMEDIR() / ".adls-md5-download-locks", parse=Path)
_CLEAN_UP_LOCKFILES_AFTER_TIME = timedelta(hours=24)
logger = log.getLogger(__name__)


def _clean_download_locks() -> int:
    deleted = 0
    deletion_threshold = time.time() - _CLEAN_UP_LOCKFILES_AFTER_TIME.total_seconds()
    try:
        for f in DOWNLOAD_LOCKS_DIR().iterdir():
            if f.is_file() and f.stat().st_mtime < deletion_threshold:
                f.unlink()
                deleted += 1
    except Exception:
        # this should be, hopefully, both very rare and completely inconsequential as to
        # program correctness.  if you see this happen multiple times, you may have some
        # read-only files or something and want to manually clean up this directory.
        logger.exception("Failed to clean download locks directory.")
    return deleted


def _occasionally_clean_download_locks():
    if random.random() < 0.005:  # do this about every 200 downloads
        # random.random is considered to be very fast, and we have no need of cryptographic quality.
        _clean_download_locks()


def download_lock(download_unique_str: str) -> FileLock:
    """Note that the lockfiles will never be deleted automatically.
    https://py-filelock.readthedocs.io/en/latest/api.html#filelock.BaseFileLock.release

    also see:
    https://stackoverflow.com/questions/58098634/why-does-the-python-filelock-library-delete-lockfiles-on-windows-but-not-unix

    This means local developers would have a whole bunch of zero-byte files in their
    download locks directory. So, we take a slightly idiosyncratic approach to cleaning
    this up: not wanting to run this code on every download, but also not wanting
    developers to see an infinitely-growing mess.  Since parallel downloads will
    (generally) not constitute a correctness issue, the 'safest' time to clean it up will
    be when you don't have any downloads in progress, but in practice it seems likely that
    we can get rid of old lockfiles after they've existed for more than 24 hours, since
    it's quite rare that a download would last that long.
    """
    DOWNLOAD_LOCKS_DIR().mkdir(parents=True, exist_ok=True)
    _occasionally_clean_download_locks()
    return FileLock(DOWNLOAD_LOCKS_DIR() / hex_md5_str(download_unique_str))
