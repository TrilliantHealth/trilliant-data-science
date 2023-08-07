import os
import typing as ty

THDS_ENV = "THDS_ENV"

Env = ty.Literal["", "prod", "dev"]


def set_active_env(env: Env):
    os.environ[THDS_ENV] = env


def get_env() -> Env:
    """Get the actual value of `THDS_ENV`. Unset == ''."""
    return ty.cast(Env, os.environ.get(THDS_ENV, ""))


def active_env(override: Env = "") -> Env:
    """Set or get the effective value of `THDS_ENV`."""
    return override or get_env() or "dev"
