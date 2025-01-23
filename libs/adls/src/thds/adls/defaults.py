"""Prefer using named_containers for new code."""

from thds.core.env import Env

from . import fqn, named_roots

try:
    import thds.adls._thds_defaults  # noqa: F401
except ImportError:
    pass


def env_root(env: Env = "") -> fqn.AdlsRoot:
    """In many cases, you may want to call this with no arguments
    to default to using the THDS_ENV environment variable.
    """
    return named_roots.require(env)


def env_root_uri(env: Env = "") -> str:
    return str(env_root(env))


def mops_root() -> str:
    """Returns a URI corresponding to the location where mops materialization should be put."""
    return str(named_roots.require("mops"))
