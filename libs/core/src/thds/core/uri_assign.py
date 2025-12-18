"""URI assignment framework "helper."

The idea is that you can have a framework or your own application hook the creation of
source.from_file (or some similar user), such that a URI can be assigned.

This decouples the 'how and whether' of URI assignment from deeper application code that
should remain agnostic about its runtime context.
"""

import typing as ty
from contextlib import contextmanager
from os import PathLike, fspath
from pathlib import Path

from thds.core import stack_context

UriAssignmentHook = ty.Callable[[ty.Union[str, PathLike]], str]


_HOOKS = stack_context.StackContext[tuple[UriAssignmentHook, ...]]("_URI_ASSIGNMENT_HOOK", tuple())


@contextmanager
def add_hook(uri_assignment_hook: UriAssignmentHook) -> ty.Iterator[None]:
    with _HOOKS.set((uri_assignment_hook, *_HOOKS())):
        yield


def for_path(path: ty.Union[str, PathLike]) -> str:  # matches UriAssignmentHook
    """API for assigning a URI to a path based on whichever registered hook first returns a non-empty URI.

    We _could_ allow path to be an optional param with default empty string, but I think the
    risks of somebody forgetting to provide a value when they actually meant to
    outweigh the convenience. You will have to be explicit about the fact that you want
    a 'directory' URI that is equivalent to the prefix.
    """
    for hook in _HOOKS():
        if uri := hook(path):
            return uri
    return ""


_DEFAULT_WORKING_DIR = Path(".out")


def replace_working_dirs_with_prefix(
    uri_prefix: str,
    path: ty.Union[str, PathLike],
    /,
    *,
    working_dirs: ty.Sequence[Path] = (_DEFAULT_WORKING_DIR,),
    # this is a standardized default, but the current working directory
    # is always the last fallback if working_dirs are empty.
) -> str:
    """If working_dirs are provided, will find the relative path from the first working_dir
    that is a parent, and will append that to the prefix provided.

    If not provided, or the path is not relative to any of the working_dirs, but the path _is_
    relative to the application current working directory, it will append the path from
    the cwd through the filename.

    If not relative to the current working directory, it will append only the name of the file.

    Intended to be used as a generic implementation of URI prefix assignment, _by_ a registered hook.
    """
    if not uri_prefix:
        return ""

    path = Path(path).resolve()
    if path == Path.cwd():
        return uri_prefix

    chosen_working_dir = Path.cwd()  # fall back to cwd
    for working_dir in working_dirs:
        working_dir = working_dir.resolve()
        if path.is_relative_to(working_dir):
            chosen_working_dir = working_dir
            break

    if not path.is_relative_to(chosen_working_dir):
        chosen_working_dir = path.parent

    assert path.is_relative_to(
        chosen_working_dir
    ), f"Path {path} is not relative to dir {chosen_working_dir}"

    relative_path = fspath(path.relative_to(chosen_working_dir))
    return f"{uri_prefix.rstrip('/')}/{fspath(relative_path).lstrip('/')}"
