"""Inspect mops control files and unpickle them for debugging.

Note that this really only works with ADLS-like Blob Stores, and
only with the MemoizingPicklingRunner, which is the only implementation
we have as of 2024-02-20, and will probably be the only implementation ever...
but if you're reading this in the distant future - those are its limitations.
"""
import argparse
import typing as ty
from dataclasses import dataclass
from pprint import pprint

from thds.adls.uri import parse_uri
from thds.core import log, scope
from thds.mops.parallel import Thunk
from thds.mops.pure.core import uris
from thds.mops.pure.core.memo import results
from thds.mops.pure.pickling._pickle import make_read_object, unfreeze_args_kwargs
from thds.mops.pure.pickling.pickles import NestedFunctionPickle
from thds.mops.pure.pickling.runner.orchestrator_side import INVOCATION
from thds.mops.srcdest.mark_remote import mark_as_remote

logger = log.getLogger(__name__)


def _unpickle_object_for_debugging(uri: str) -> ty.Any:
    try:
        if uri.endswith("/" + INVOCATION):
            nested = ty.cast(NestedFunctionPickle, make_read_object(INVOCATION)(uri))
            args, kwargs = mark_as_remote(unfreeze_args_kwargs(nested.args_kwargs_pickle))
            return Thunk(nested.f, args, kwargs)
        return make_read_object("output")(uri)
    except ImportError as ie:
        logger.error(f"Could not import the module ({ie}) needed to unpickle the object.")
        logger.error("Try re-running this tool in the environment where the above module is available.")
        raise


def _resolved_uri(uri: str) -> str:
    if not uri:
        return ""
    return str(parse_uri(uri))


_KNOWN_CONTROL_FILES = list(map(lambda cf: "/" + cf, [INVOCATION, results.RESULT, results.EXCEPTION]))
# prefix with forward-slash because these live in a blob store 'directory'


@dataclass
class IRE:
    invocation: ty.Any
    result: ty.Any
    exception: ty.Any


_NOTHING = object()


@scope.bound
def get_control_file(uri: str) -> ty.Any:
    """Returns _NOTHING if 'normal' errors occur."""
    try:
        uri = _resolved_uri(uri)
    except Exception as e:
        logger.error(f"Error while resolving {uri}: {e}")
        return _NOTHING

    if not any(uri.endswith(control_file) for control_file in _KNOWN_CONTROL_FILES):
        store = uris.lookup_blob_store(uri)
        logger.info(f"Attempting to fetch all control files for {uri}")
        return IRE(
            **{cf.lstrip("/"): get_control_file(store.join(uri, cf)) for cf in _KNOWN_CONTROL_FILES}
        )

    no_warning = bool(uris.ACTIVE_STORAGE_ROOT())
    try:
        scope.enter(uris.ACTIVE_STORAGE_ROOT.set(uris.get_root(uri)))
        return _unpickle_object_for_debugging(uri)
    except Exception as e:
        if uris.lookup_blob_store(uri).is_blob_not_found(e):
            if no_warning:
                logger.warning(f"Could not find an object at the URI {uri}.")
            return None
        logger.exception("Unexpected error while unpickling the object.")
        raise


def _embed(o):
    print('\nObject will be available as "o". Perform embedded URI fetches with "get_control_file"\n')
    try:
        __import__("IPython").embed()
    except ImportError:
        print("IPython not found, falling back to standard Python shell.")
        import code

        code.interact(local=locals())


def _inspect(uri: str, embed: bool = True):
    obj = get_control_file(uri)
    if obj is _NOTHING:
        return

    if embed:
        _embed(obj)
    else:
        print()
        pprint(obj, indent=4, width=60, sort_dicts=False)
    return obj


def main():
    parser = argparse.ArgumentParser(description="Inspect a pickled mops invocation.")
    parser.add_argument("uri", type=str, help="The URI of the object to inspect.")
    parser.add_argument("--embed", action="store_true", help="Embed an IPython shell after inspection.")
    parser.add_argument("--loop", action="store_true", help="Keep prompting for URIs to inspect.")
    args = parser.parse_args()

    _inspect(args.uri, args.embed)

    if args.loop:
        prompt = "\nEnter another URI to inspect, or empty string to exit: "
        uri = input(prompt)
        while uri:
            _inspect(uri, args.embed)
            uri = input(prompt)


if __name__ == "__main__":
    main()
