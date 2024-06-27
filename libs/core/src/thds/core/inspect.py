import inspect

import attrs


@attrs.frozen
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
    parentframe = stack[start][0]

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
