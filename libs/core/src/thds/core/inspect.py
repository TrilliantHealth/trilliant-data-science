import inspect
import typing as ty
from dataclasses import dataclass
from types import ModuleType


@dataclass(frozen=True)
class CallerInfo:
    module: str = ""
    klass: str = ""
    caller: str = ""
    line: int = 0


def get_caller_info(skip: int = 2) -> CallerInfo:
    # Credit: https://gist.github.com/lee-pai-long/d3004225e1847b84acb4fbba0c2aea91
    # I have made some small modifications to the code
    """Get the name of a caller in the format module.class.method.
    Copied from: https://gist.github.com/techtonik/2151727
    :arguments:
        - skip (integer): Specifies how many levels of stack
                          to skip while getting caller name.
                          skip=1 means "who calls me",
                          skip=2 "who calls my caller" etc.
    :returns:
        - module (string): full dotted name of caller module.
        - klass (string): caller classname if one otherwise None.
        - caller (string): caller function or method (if a class exist).
        - line (int): the line of the call.
        - An empty string is returned if skipped levels exceed stack height.
    """
    stack = inspect.stack()
    start = 0 + skip
    if len(stack) < start + 1:
        raise RuntimeError(f"The stack has less than f{skip} + 1 frames in it.")
    parentframe = stack[start].frame

    # full dotted name of caller module
    module_info = inspect.getmodule(parentframe)
    module = module_info.__name__ if module_info else ""

    # class name
    klass = ""
    if "self" in parentframe.f_locals:
        klass = parentframe.f_locals["self"].__class__.__name__

    # method or function name
    caller = ""
    if parentframe.f_code.co_name != "<module>":  # top level usually
        caller = parentframe.f_code.co_name

    # call line
    line = parentframe.f_lineno

    # Remove reference to frame
    # See: https://docs.python.org/3/library/inspect.html#the-interpreter-stack
    del parentframe

    return CallerInfo(module=module, klass=klass, caller=caller, line=line)


def bind_arguments(
    func: ty.Callable, /, *args: ty.Sequence, **kwargs: ty.Mapping[str, ty.Any]
) -> inspect.BoundArguments:
    bound = inspect.signature(func).bind(*args, **kwargs)
    bound.apply_defaults()
    return bound


def get_argument(arg_name: str, bound_arguments: inspect.BoundArguments) -> ty.Any:
    return bound_arguments.arguments[arg_name]


def yield_caller_modules_and_frames(*skip: str) -> ty.Iterator[tuple[ModuleType, inspect.FrameInfo]]:
    """Yields caller modules and their frame info, skipping any modules in the skip list."""
    stack = inspect.stack()
    skip = set(skip) | {__name__}  # type: ignore
    for frame_info in stack[1:]:  # don't bother with the current frame, obviously
        module = inspect.getmodule(frame_info.frame)
        if module:
            module_name = module.__name__
            if module_name not in skip:
                yield module, frame_info


def caller_module_name(*skip: str) -> str:
    """
    Find the first caller module that is not in the skip list.
    :param skip: module names to skip
    :return: the first caller module name not in skip, or empty string if no module can be found
    """
    for module, _frame in yield_caller_modules_and_frames(*skip):
        return module.__name__

    return ""  # this is trivially distinguishable from a module name, so no need to force people to handle None
