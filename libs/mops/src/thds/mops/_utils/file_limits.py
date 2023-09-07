"""Automatically manage process resource limits."""

from .. import config


def set_file_limit(n: int):
    """Works like calling `ulimit -Sn <N>` on a Mac."""
    import resource  # noqa

    resource.setrlimit(resource.RLIMIT_NOFILE, (n, n))
    assert resource.getrlimit(resource.RLIMIT_NOFILE) == (n, n)


def bump_limits():
    """It was common to have to do this manually on our macs. Now that is no longer required."""
    set_file_limit(config.open_files_limit())
