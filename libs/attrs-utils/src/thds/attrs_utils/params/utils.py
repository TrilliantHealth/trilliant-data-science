import typing as ty

import typing_inspect as ti

TypeOrVar = ty.Union[ty.Type, ty.TypeVar]


def origin_params_and_args(
    type_: ty.Type,
) -> ty.Tuple[ty.Type, ty.Tuple[ty.TypeVar, ...], ty.Tuple[TypeOrVar, ...], bool]:
    """Get the origin, parameters, and arguments of a generic type.
    When a generic type has not been parameterized, the arguments will be the same as the parameters.
    Examples:
        - For `List[int]`, returns `(list, (T,), (int,))`
        - For `Dict[str, U]`, returns `(dict, (K, V), (str, U))`
        - For just `List`, returns `(list, (T,), (T,))`
    """
    if args := ti.get_args(type_):
        origin = ti.get_origin(type_)
        params = ti.get_parameters(origin)
        parameterized = True
    else:
        origin = type_
        params = ti.get_parameters(origin)
        args = params
        parameterized = False
    return origin, params, args, parameterized
