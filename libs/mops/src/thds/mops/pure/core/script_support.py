"""Support for transferring execution of a function
when the function is defined inside the __main__ module.

Only works if you 'transfer execution' to the same process.
"""
import typing as ty

_LOCAL_MAIN_FUNCTIONS: ty.Dict[str, ty.Callable] = dict()


def add_main_module_function(function_name: str, function: ty.Callable) -> None:
    """This only works if you end up running remotely in the same process."""
    _LOCAL_MAIN_FUNCTIONS[function_name] = function


def get_main_module_function(fname: str) -> ty.Callable:
    """This only works if you end up running 'remotely' in the same process."""
    try:
        return _LOCAL_MAIN_FUNCTIONS[fname]
    except KeyError:
        raise ValueError(
            f"Serialized function {fname} that was in the __main__ module"
            " and attempted to transfer control to a different process."
            " Please move your function to a module that is not __main__."
        ) from KeyError
