"""A 'scale group' is a string that represents a configuration 'grouping' various systems
by a shared input scaling factor.

This module only provides a way for other modules to add to the active set of scale groups.
Other modules/projects must provide their own meaning for these names.
"""

import contextlib
import typing as ty

from thds.core.stack_context import StackContext

_SCALE_GROUP_PRIORITY: StackContext[ty.Tuple[str, ...]] = StackContext("_SCALE_GROUP_PRIORITY", ("",))


@contextlib.contextmanager
def push_scale_group(*scale_group_names: str) -> ty.Iterator[ty.Tuple[str, ...]]:
    """Puts some scale group strings at the head of the list, such that they will be
    'found first', before all other scale groups. First argument is the highest overall priority.

    The scale groups are popped off the stack when the context manager exits.

    You may want to use this in conjunction with `core.scope.enter` (and
    `core.scope.bound`) in order to avoid introducing extra layers of nesting, while still
    making sure that you only set the scale group for your context.
    """
    for scale_group_name in scale_group_names:
        if not scale_group_name:
            raise ValueError(f"Scale group name must be a non-empty string; got {scale_group_names}")

    with _SCALE_GROUP_PRIORITY.set(
        (*scale_group_names, *(sz for sz in _SCALE_GROUP_PRIORITY() if sz not in scale_group_names))
    ):
        yield _SCALE_GROUP_PRIORITY()


def active_scale_groups() -> ty.Tuple[str, ...]:
    """Returns the active scale groups, in order of priority."""
    return _SCALE_GROUP_PRIORITY()
