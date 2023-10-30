import inspect
import typing as ty

from ..types import Args, Kwargs

F = ty.TypeVar("F", bound=ty.Callable)


def argument_overwriter(
    func: F,
    names_to_callables: ty.Mapping[str, ty.Callable[[ty.Any], ty.Any]],
) -> ty.Callable[[Args, Kwargs], ty.Tuple[Args, Kwargs]]:
    signature = inspect.signature(func)
    parameter_names = list(signature.parameters.keys())

    def overwriter(args: Args, kwargs: Kwargs) -> ty.Tuple[Args, Kwargs]:
        # Iterate over positional arguments and replace named ones with None
        pos_args = list(args)  # mutable copy
        kwargs = dict(kwargs)  # mutable copy
        for i, arg in enumerate(args):
            if i < len(parameter_names):
                param_name = parameter_names[i]
                if param_name in names_to_callables:
                    pos_args[i] = names_to_callables[param_name](arg)

        # Iterate over keyword arguments and replace named ones with None
        for param_name, arg in kwargs.items():
            if param_name in names_to_callables:
                kwargs[param_name] = names_to_callables[param_name](arg)

        return pos_args, kwargs

    return overwriter


def parameter_overwriter(
    func: F, names_to_values: ty.Mapping[str, ty.Any]
) -> ty.Callable[[Args, Kwargs], ty.Tuple[Args, Kwargs]]:
    def give_val(val) -> ty.Callable[[ty.Any], ty.Any]:
        return lambda _: val

    return argument_overwriter(func, {name: give_val(val) for name, val in names_to_values.items()})
