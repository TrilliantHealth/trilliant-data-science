"""Logger context carried across process boundaries by the environment.

`logger_context` is a ContextVar, so it reaches threads but not children: a process that
launches work in subprocesses cannot tag their logs with what that work *is*. That matters
wherever one process supervises many concurrent children - their output interleaves with
nothing to filter on.

The environment crosses that boundary. A launcher calls `as_env` to build the variables
and passes them to the child; the child's `thds.core.log` seeds its base context from them
at import, so every line it logs carries them, and a `logger_context` it enters later
nests on top as usual.

    subprocess.run(cmd, env={**os.environ, **log.env.as_env(item=item_id)})

Context set this way is inherited by the whole process tree, which is normally what you
want - a child's own helpers describe the same work. To stop it at a boundary, spawn with
the environment `suppress` returns.

One key per variable, rather than one packed variable, so a caller can drop a single key.

These values are as trusted as the environment the process was launched with, which is to
say less than a value set in-process. Log context is not an access-control decision, but
don't derive one from it either.
"""

import os
import typing as ty

PREFIX = "THDS_CORE_LOG_CONTEXT_"
# A key becomes PREFIX + the key uppercased; as_env/from_env are inverses over the keys
# `logger_context` accepts, which are Python identifiers.


def _var(key: str) -> str:
    return PREFIX + key.upper()


def as_env(**context: ty.Any) -> ty.Dict[str, str]:
    """Environment variables that seed `context` as a child's base logger context.

    Merge into the environment you hand the child; it carries no other variables, so it
    composes with whatever else that child needs.
    """
    return {_var(key): str(value) for key, value in context.items()}


def suppress(environ: ty.Optional[ty.Mapping[str, str]] = None, *keys: str) -> ty.Dict[str, str]:
    """A copy of `environ` (default: this process's) without inherited logger context.

    Naming no keys drops all of it, which is the usual case at a boundary where the child's
    work is unrelated to ours. Naming keys drops only those, leaving the rest to describe
    what the child is still part of.
    """
    dropping = {_var(key) for key in keys} if keys else None
    return {
        name: value
        for name, value in (os.environ if environ is None else environ).items()
        if not (name.startswith(PREFIX) and (dropping is None or name in dropping))
    }


def from_env(environ: ty.Optional[ty.Mapping[str, str]] = None) -> ty.Dict[str, str]:
    """The logger context carried in `environ` (default: this process's).

    Called once at import to seed the base context; call it again in a test that sets the
    variables after import, since nothing re-reads the environment on its own.
    """
    return {
        name[len(PREFIX) :].lower(): value
        for name, value in (os.environ if environ is None else environ).items()
        if name.startswith(PREFIX)
    }
