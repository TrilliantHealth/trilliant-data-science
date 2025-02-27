"""Provides for precise cache invalidation via a known key in the docstring.

`function-logic-key`:

By modifying this key in the docstring of a use_runner-decorated
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
import typing as ty
from functools import lru_cache

from thds.mops._utils.names import full_name_and_callable

_DOCSTRING_VERSION_RE = re.compile(r".*function-logic-key:\s+(?P<version>[^\s]+)\b", re.DOTALL)


def _parse_logic_key(doc: str) -> str:
    m = _DOCSTRING_VERSION_RE.match(doc)
    return m.group("version") if m else ""


def extract_function_logic_key_from_docstr(obj: ty.Any) -> str:
    doc = getattr(obj, "__doc__", "") or ""
    return _parse_logic_key(doc)


extract_logic_key_from_docstr = extract_function_logic_key_from_docstr


@lru_cache(maxsize=None)
def make_unique_name_including_docstring_key(f: ty.Any) -> str:
    module_and_name, callable = full_name_and_callable(f)
    version = ""
    for attr, value in inspect.getmembers(callable):
        if attr == "__doc__" and value:
            version = _parse_logic_key(value)
    return f"{module_and_name}@{version}".rstrip("@")


class FunctionComponents(ty.NamedTuple):
    module: str
    name: str
    function_logic_key: str


def parse_unique_name(full_function_name: str) -> FunctionComponents:
    assert "--" in full_function_name, f"Expected '--' in {full_function_name}"
    module, name = full_function_name.split("--")
    if "@" not in name:
        return FunctionComponents(module, name, "")
    return FunctionComponents(module, *name.split("@"))
