"""Provides for precise cache invalidation via a known key in the docstring.

`function-logic-key`:

By modifying this key in the docstring of a pure_remote-decorated
function or callable class, you can indicate to mops that although the
function name has not changed (perhaps because of refactoring
concerns), and although the parameters may be the same and take the
same values as a previous run, nevertheless the (value of the)
internal logic has changed and therefore no previously memoized
results should be returned when running this function.

This step is entirely optional and is expected only to be used for
advanced use cases where optimal caching/memoization is required.

The keys may be any string without spaces, but ideally should be
semantically meaningful to another developer or yourself, i.e. a name
or description of some sort. Examples might be:

function-logic-key: v1
function-logic-key: 2023-03-31
function-logic-key: try-uint8-math

"""
import inspect
import re
from functools import lru_cache

_DOCSTRING_VERSION_RE = re.compile(r".*function-logic-key:\s+(?P<version>[^\s]+)\b", re.DOTALL)


@lru_cache(maxsize=None)
def make_unique_name_including_docstring_key(f) -> str:
    module = ""
    name = ""
    version = ""
    for attr, value in inspect.getmembers(f):
        if attr == "__doc__" and value:
            m = _DOCSTRING_VERSION_RE.match(value)
            if m:
                version = m.group("version")
        elif attr == "__module__":
            module = value
        elif not name and attr == "__name__":
            name = value
    if not name:
        try:
            # support functools.partial
            return make_unique_name_including_docstring_key(f.func)
        except AttributeError:
            pass
        try:
            # for some reason, __name__ does not exist on instances of objects,
            # nor does it exist as a 'member' of the __class__ attribute, but
            # we can just pull it out directly like this for callable classes.
            name = f.__class__.__name__
        except AttributeError:
            name = "MOPS_UNKNOWN_NAME"
    return f"{module}:{name}@{version}".rstrip("@")
