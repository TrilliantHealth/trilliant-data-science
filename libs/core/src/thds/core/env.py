import os
import typing as ty

Env = ty.Literal["", "prod", "dev"]


def set_active_env(env: Env):
    os.environ["THDS_ENV"] = env


def _get_env() -> Env:
    return ty.cast(Env, os.environ.get("THDS_ENV") or "dev")


def active_env(override: Env = "") -> Env:
    return override or _get_env() or "dev"
