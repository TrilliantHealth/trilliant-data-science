"""Where a remote is running, described by whatever launched it.

An event says *that* work started; this says *where*, in terms only the launching runtime
can supply - a pod and namespace on Kubernetes, a cluster and task on Databricks, a pid
in a subprocess. `mops` core cannot name any of those without knowing about every runtime
that might ever exist, so it does not: it asks for a mapping and records what it gets.

The provider is named by a dotted import path in the environment because that is the only
thing that crosses a process boundary - a shim is a callable on the *launching* side
(`Callable[[Sequence[str]], ...]`, see `pure.runner.types`), and it has no remote half to
hang a method on. Widening that signature would break every shim ever written, including
ones outside this repository, so the launcher exports a name instead and the remote
imports it. `mops.metadata.extra_generator` already crosses the same boundary the same
way, for the same reason.

Shipping a provider means: write `() -> RuntimeContext`, and have the launcher set
`MOPS_RUNTIME_CONTEXT` to its dotted path in the remote's environment. Nothing in `mops`
core needs to learn the runtime's name, and a reader that does not recognise it still has
key/value pairs to show.
"""

import importlib
import typing as ty

from thds.core import config, log

RUNTIME_CONTEXT_PROVIDER = config.item("mops.runtime_context", default="")
# Dotted import path to a callable returning a RuntimeContext, resolved on the remote.
# The launcher is responsible for setting this in the remote's environment; each of
# mops's own launchers defaults it to its own provider.
#
# This name derives the environment variable MOPS_RUNTIME_CONTEXT, which is what the
# launchers set. Renaming it breaks that correspondence silently: remotes report no
# coordinates and nothing raises.

logger = log.getLogger(__name__)


class RuntimeContext(ty.NamedTuple):
    """What ran the work, and where to find it.

    `runtime` names the launching runtime ("k8s", "databricks", ...) and is what a reader
    keys its rendering on. `coordinates` is whatever addresses this execution within that
    runtime; a reader that knows the runtime interprets them, and one that does not
    displays them as they are, which is why the values are strings.
    """

    runtime: str
    coordinates: ty.Mapping[str, str]


EMPTY = RuntimeContext("", {})

RuntimeContextProvider = ty.Callable[[], RuntimeContext]


def _load(import_path: str) -> None | RuntimeContextProvider:
    try:
        module_path, name = import_path.rsplit(".", 1)
        return ty.cast(RuntimeContextProvider, getattr(importlib.import_module(module_path), name))
    except (ValueError, ImportError, AttributeError):
        logger.warning("Could not load the runtime context provider '%s'.", import_path)
        return None


def current() -> RuntimeContext:
    """This process's runtime context, or `EMPTY` when nothing describes it.

    Never raises. A provider that fails costs a reader the address of one execution; it
    must not cost the invocation, which is already running by the time this is called.
    """
    import_path = RUNTIME_CONTEXT_PROVIDER()
    if not import_path:
        return EMPTY

    provider = _load(import_path)
    if not provider:
        return EMPTY

    try:
        context = provider()
    except Exception:
        logger.warning("The runtime context provider '%s' raised.", import_path, exc_info=True)
        return EMPTY

    return RuntimeContext(context.runtime, {k: v for k, v in context.coordinates.items() if v})
    # empty values dropped here rather than by each provider: a provider reads its
    # environment, and an unset variable is the normal way for a coordinate not to apply.
