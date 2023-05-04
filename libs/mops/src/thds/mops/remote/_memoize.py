"""A big part of what mops offers is automatic memoization.

It's built on the principle that if we need to be able to transfer
execution from one system/environment to another, then by definition
your computation must be a pure function, otherwise the result is not
reliable. And, because it _is_ a pure function, by definition we can
memoize your calls to it. More than that, we already _have_ memoized
them, because in order to transfer the invocation to the worker
environment, and then the worker's results back to your orchestrator,
we needed to serialize them somewhere, and those serialized invocation
and result will (in theory) be there the next time we look for them.

In a perfect world with pure functions, this memoization would be
omnipresent and completely transparent to the user. However, we don't
live in a perfect world. There are at least two common ways in which
always-on memoization could lead to incorrect behavior:

1. Your code changes between calls to the same function.

   We can't reliably detect this, because we're not actually able to
   serialize or otherwise derive a key from the full code,
   recursively, of your function and everything it
   references/calls.

   Therefore, we allow you to notify us of these changes in one of
   several ways, but the most common is by using mops without
   explicitly setting a `pipeline_id` for your application's run.

   If you don't set a `pipeline_id`, then one will be
   non-deterministically generated for you at every application start;
   essentially, you'll get no memoization of any kind, because you
   haven't confirmed (via pipeline_id) that your code has not
   changed. But if you do set the same pipeline_id consistently when
   running your function, you'll be able to take advantage of the
   memoization that is already occurring under the hood.

2. Your function writes its true results as side effects to some other
   storage location, and the returned result from the function merely
   _references_ the true result, which is stored in that external
   system.

   In other words, your function is not truly pure.

   In this case, the actual source of erroneous behavior would be if
   the external storage system is mutable. If it is not mutable, or
   if, by convention, the storage can reliably be treated as
   representing immutable, persistent data, then aside from network
   errors or other sources of retryable non-determinism, your
   application can be expected to reliably reuse memoized results from
   this technically impure but pure-in-practice function.

   In general, this source of non-determinism is probably the easier
   to deal with, as it requires only the one convention - namely, that
   certain ADLS storage accounts/containers should never have new and
   different data written over top of existing data.


The code that follows helps address point #1 above. Code changes are
endemic to software development and data science, and it cannot be
expected that memoization will only be used after code is "set in
stone".

The approach taken here is that it should be possible to run a given
process, with a known or even an auto-generated pipeline id, and then
simply record that pipeline id for later, such that a future caller of
the function can opt into the memoized results of that 'known run'
simply by calling the function.

The implementation detail is that this will be done out of band -
instead of modifying the code (either the called code or the call
site), we will allow this to be 'injected' via configuration, on a
per-function (rather than per-application, or per-function-call)
basis.

- per-application is rejected because it's what pipeline_id already
  does - if you simply want to opt in to an entire 'universe' of
  memoized results, you can reuse the pipeline_id corresponding to
  that universe. We're trying to solve for a case where multiple
  'universes' need to be stitched together in a later re-use of
  memoized results.  - per-function-call is rejected because there are
  no currently-anticipated use cases for it - as an implementation
  detail this would not be particularly hard to achieve, but it also
  seems likely to be more 'developer overhead' than anybody would
  really want to use in practice.

The memoization/cache key for `pure_remote` (mops) function calls is made up of three parts or levels:

- The top level is the global storage config, including SA, container,
  and a version-specific base path provided by the `mops` runner.
  This level is not semantically derived from the function call
  itself; it's present purely as a technical reality.

  In the configuration and in the code, the configurable part of this
  is referred to as the storage_root. Once a mops runner adds its own
  base path, it becomes the runner prefix.

- The middle level is the 'code' memoization, which provides users granular ways of
  invalidating caches across runs sharing a runner prefix by changing one of:
---- pipeline_id
---- name of function being memoized
---- cache key in docstring for function being memoized
  to indicate that something about the _code being run_ has changed.

- The bottom level is the 'arguments' memoization,
  whereby we serialize and then hash the full set of arguments to the function,
  such that different calls to the same function will memoize differently as expected.

Of the three levels, our per-function memoization config should only need to 'deal' with the top two levels.

- A previous call to the function in question might have used a
  different storage root than is configured by the application for the
  default case, so it must be necessary to specify where we want to
  look for memoized results.

- The pipeline_id used for a known result may be different for various
  different functions that we intend to call.

- If a codebase has undergone refactoring, such that a function lives
  in a different module than it previously did, but you wish to reuse
  memoized results, it should be possible to provide a translation
  layer for the name itself.

- In rare cases, the (optional) value of a function's
  function-logic-key (embedded in the docstring) may have changed
  compared to the version we're able to import, but we may still wish
  to pick up the result of a different configuration.

Notably, we do _not_ propose to allow configuration of the hashed
args/kwargs itself, which would amount to a full redirect of the
function call to a known result. It's not that there might not be some
use case for this functionality; we simply don't foresee what that
would be and decline to prematurely implement such functionality.

"""
import hashlib
import typing as ty
from functools import reduce

from ..config import config_at_path
from ._hash import nest
from ._memoize_function_code import make_unique_name_including_docstring_key
from ._root import get_pipeline_id
from ._uris import lookup_blob_store
from .memoize import get_pipeline_id_mask


def _lookup_memospace(callable_name: str) -> ty.Optional[str]:
    """The base URI is everything up until but not including the hash of the (args, kwargs) tuple."""
    return config_at_path(None, "mops", "memo", callable_name, "memospace")


def get_mask_or_pipeline_id() -> str:
    return get_pipeline_id_mask() or get_pipeline_id()


def make_function_memospace(default_storage_root: str, f: ty.Callable) -> str:
    callable_name = make_unique_name_including_docstring_key(f)
    return _lookup_memospace(callable_name) or reduce(
        lookup_blob_store(default_storage_root).join,
        (
            default_storage_root,
            get_mask_or_pipeline_id(),
            callable_name,
        ),
    )


def args_kwargs_content_address(args_kwargs_bytes: bytes) -> str:
    return nest(hashlib.sha256(args_kwargs_bytes).hexdigest())
