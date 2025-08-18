import typing as ty

F = ty.TypeVar("F", bound=ty.Callable)


@ty.overload
def compose(*decorators: ty.Callable[[F], F]) -> ty.Callable[[F], F]:
    ...  # pragma: no cover


@ty.overload
def compose(*decorators: ty.Callable[[F], F], f: F) -> F:
    ...  # pragma: no cover


def compose(*decorators: ty.Callable[[F], F], f: ty.Optional[F] = None) -> ty.Callable[[F], F]:
    """A decorator factory that creates a single decorator from
    multiple, following the top-to-bottom order that would be used by
    the `@deco` decorator syntax, which is actually standard R-to-L
    composition order.

    It may also be used to both compose the decorator and apply it
    directly to a provided function.

    This is useful when you want to apply a sequence of decorators
    (higher-order functions) to a function, but you can't use the
    decorator syntax because you want the original function to be
    callable without the decorators.

    Example:

    >>> decos.compose_partial(
    ...     deco1,
    ...     deco2,
    ...     deco3,
    ... )(f)

    is equivalent to:

    >>> @deco1
    ... @deco2
    ... @deco3
    ... def f():
    ...     pass

    """

    def _deco(func: F) -> F:
        for deco in reversed(decorators):
            func = deco(func)
        return func

    if f:
        return _deco(f)
    return _deco
