"""This module currently exists only to serve the use case of tracking function logic keys recursively
for mops-wrapped functions that call other mops-wrapped functions...

which is _often but not always_ an anti-pattern...
"""

import typing as ty

from .unique_name_for_function import (
    extract_function_logic_key_from_docstr,
    make_unique_name_including_docstring_key,
)


def resolve(
    calls_registry: ty.Mapping[ty.Callable, ty.Collection[ty.Callable]],
    origin_callable: ty.Callable,
) -> ty.List[ty.Callable]:
    """Using the 'edges' defined in the mapping, return a set of all callables recursively
    reachable from the origin callable, not including the origin callable itself.
    """
    visited = list()
    stack = [origin_callable]

    while stack:
        current_function = stack.pop()
        if current_function in visited:
            continue

        if current_function is not origin_callable:
            visited.append(current_function)

        stack.extend(calls_registry.get(current_function, []))

    return visited


# the below code is operating under the assumption that putting all function full names
# (module and name) plus their function logic keys inside the memo uri explicitly is
# better than hashing them all together, because this gives users the standard amount of
# 'debuggability' that they've come to expect when it comes to things like function names
# and function-logic-keys.
#
# it _will_ lead to longer memo uris, which is unfortunate in some ways.
#
# an alternative would be to store these inside the Invocation and also change the _hash_
# that we're already computing from being a hash of _only_ the args, kwargs to a hash
# of those plus these.
#
# but there's great utility in being able to see that the hash itself comes only
# from the args, kwargs - and it is not expected that the set of functions 'called'
# would change during runtime - it should be static like everything else.
#
# A third alternative would be to use a second hash (calls-<the hash>) and then embed the
# actual names in some other place - either the invocation itself, or possibly as
# metadata. But I also think there's utility in being able to see those function names as
# part of the overall memo uri, rather than having to go look at metadata to see it.  It
# also makes it much more obvious that we're doing nesting - which is frowned upon for
# most use cases, and good to make visible even if the use case is valuable.


CALLS_PREFIX = "calls-"


def combine_function_logic_keys(functions: ty.Iterable[ty.Callable]) -> tuple[str, ...]:
    funcs_and_logic_keys = list()
    for func in functions:
        flk = extract_function_logic_key_from_docstr(func)
        if flk:
            # if the function doesn't have a function logic key, then it can't really 'invalidate'
            # anything, so we can ignore it.
            funcs_and_logic_keys.append(CALLS_PREFIX + make_unique_name_including_docstring_key(func))
    return tuple(sorted(funcs_and_logic_keys))


class CallsPieces(ty.NamedTuple):
    remaining_prefix: str
    full_function_name: str
    calls_functions: list[str]


def split_off_calls_strings(memo_str_not_including_args_kwargs: str, separator: str) -> CallsPieces:
    calls_functions = list()
    rest = memo_str_not_including_args_kwargs
    while True:
        rest, full_function_name = rest.rsplit(separator, 1)
        if full_function_name.startswith(CALLS_PREFIX):
            calls_functions.append(full_function_name[len(CALLS_PREFIX) :])
        else:
            break
    return CallsPieces(rest, full_function_name, calls_functions)
