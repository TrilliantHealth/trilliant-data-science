"""Trilliant Health data science team core utils"""
from . import (  # noqa: F401
    cache,
    concurrency,
    config,
    decos,
    dict_utils,
    env,
    exit_after,
    files,
    fretry,
    generators,
    git,
    hash_cache,
    hashing,
    home,
    hostname,
    imports,
    inspect,
    lazy,
    link,
    log,
    merge_args,
    meta,
    prof,
    scope,
    source,
    sqlite,
    stack_context,
    thunks,
    timer,
    tmp,
    types,
)

# these imports are helpful for IDE to parse things `core` usage like, `from thds import core`...`core.log.getLogger`
# this list of imports has no effect on runtime behavior and keeping this up to date is just a nicety and not *required*

__version__ = meta.get_version(__name__)
metadata = meta.read_metadata(__name__)
__commit__ = metadata.git_commit
