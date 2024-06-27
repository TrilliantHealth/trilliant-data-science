"""Defaults for Data Science"""
from thds.core.env import Env, active_env

from .fqn import AdlsRoot

_PROD_DATASETS = AdlsRoot.parse("adls://thdsdatasets/prod-datasets")
_TMP = AdlsRoot.parse("adls://thdsscratch/tmp")


def env_root(env: Env = "") -> AdlsRoot:
    """In many cases, you may want to call this with no arguments
    to default to using the THDS_ENV environment variable.
    """
    env = active_env(env)
    if env == "prod":
        return _PROD_DATASETS
    if env == "dev":
        return _TMP
    return _TMP


def env_root_uri(env: Env = "") -> str:
    return str(env_root(env))