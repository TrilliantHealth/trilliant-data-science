import os
import typing as ty
from contextlib import contextmanager

from .stack_context import StackContext

THDS_ENV = "THDS_ENV"

Env = ty.Literal["", "prod", "dev", "ua-prod"]

_TEMP_ENV: StackContext[Env] = StackContext("thds-env", "")


def set_active_env(env: Env):
    os.environ[THDS_ENV] = env


def get_raw_env() -> Env:
    """Get the actual value of `THDS_ENV`. Unset == ''

    Prefer `active_env` for determining the active environment.
    """
    return ty.cast(Env, os.environ.get(THDS_ENV, ""))


@contextmanager
def temp_env(env: Env = "") -> ty.Iterator[Env]:
    """Temporarily set the value of `THDS_ENV` for the current stack/thread.

    Useful if you have special cases where you need to fetch a result
    from a different environment than the one you're intending to run,
    without affecting the global state of your application.
    """
    with _TEMP_ENV.set(env or active_env()):
        yield _TEMP_ENV()


def active_env(override: Env = "") -> Env:
    """Get the effective value of `THDS_ENV`."""
    return override or _TEMP_ENV() or get_raw_env() or "dev"
