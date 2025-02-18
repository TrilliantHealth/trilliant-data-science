import inspect
import typing as ty

from ..types import Args, Kwargs

F = ty.TypeVar("F", bound=ty.Callable)


def argument_transformer(
    func: F,
    names_to_transforms: ty.Mapping[str, ty.Callable[[ty.Any], ty.Any]],
) -> ty.Callable[[Args, Kwargs], ty.Tuple[Args, Kwargs]]:
    """Overwrite bound arguments at call time with the value returned
    by each named transform, whose names must each correspond to a
    named parameter on the function.

    The named transforms will each receive the 'raw' argument value,
    and may choose whether to return it or something else. A mapping
    of identity functions (foo=lambda x: x) would be, therefore,
    functionally a no-op.

    This not a decorator, but a decorator can easily be built on top of it.
    """
    signature = inspect.signature(func)
    parameter_names = list(signature.parameters.keys())
    unknown_parameters = set(names_to_transforms) - set(parameter_names)
    if unknown_parameters:
        # don't let this bad situation go any further...
        raise ValueError(f"The function {func} does not have parameters {unknown_parameters}")

    def xf_args_kwargs(args: Args, kwargs: Kwargs) -> ty.Tuple[Args, Kwargs]:
        # Iterate over positional arguments and replace named ones
        # with the value received from the callable.
        pos_args = list(args)  # mutable copy
        for i, arg in enumerate(args):
            if i < len(parameter_names):
                param_name = parameter_names[i]
                if param_name in names_to_transforms:
                    pos_args[i] = names_to_transforms[param_name](arg)

        # Iterate over keyword arguments and replace named ones
        # with the value received from the callable.
        kwargs = dict(kwargs)  # mutable copy
        for param_name, arg in kwargs.items():
            if param_name in names_to_transforms:
                kwargs[param_name] = names_to_transforms[param_name](arg)

        return pos_args, kwargs

    return xf_args_kwargs


def parameter_overwriter(
    func: F, names_to_values: ty.Mapping[str, ty.Any]
) -> ty.Callable[[Args, Kwargs], ty.Tuple[Args, Kwargs]]:
    """Overwrite parameters without regard to the actual argument values."""

    def give_val(val: ty.Any) -> ty.Callable[[ty.Any], ty.Any]:
        return lambda _: val

    return argument_transformer(func, {name: give_val(val) for name, val in names_to_values.items()})
