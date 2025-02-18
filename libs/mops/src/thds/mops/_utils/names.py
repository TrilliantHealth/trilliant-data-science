import typing as ty


def full_name_and_callable(func: ty.Any) -> ty.Tuple[str, ty.Callable]:
    """return {module}--{name} for an actual (non-wrapped) function or class,
    plus the unwrapped callable itself.
    """
    if hasattr(func, "func"):  # support functools.partial
        return full_name_and_callable(func.func)

    module = func.__module__
    try:
        name = func.__name__
    except AttributeError:
        try:
            # for some reason, __name__ does not exist on instances of objects,
            # nor does it exist as a 'member' of the __class__ attribute, but
            # we can just pull it out directly like this for callable classes.
            name = func.__class__.__name__
        except AttributeError:
            name = "MOPS_UNKNOWN_NAME"

    return f"{module}--{name}", func
