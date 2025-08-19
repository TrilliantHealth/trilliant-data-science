"""Allows avoiding prop-drilling of contextual information.
Instead of threading an argument through many layers of functions,
create a global StackContext, and use a with statement to set its
value for everything below the current place on the stack.
Only affects your thread/green thread (works with async).
"""
import contextlib as cl
import contextvars as cv
import typing as ty

T = ty.TypeVar("T")
F = ty.TypeVar("F", bound=ty.Callable)


@cl.contextmanager
def stack_context(contextvar: cv.ContextVar[T], value: T) -> ty.Iterator[T]:
    try:
        token = contextvar.set(value)
        yield value
    finally:
        contextvar.reset(token)


class StackContext(ty.Generic[T]):
    """A thin wrapper around a ContextVar that requires it to be set in a
    stack-frame limited manner.
    These should only be created at a module/global level, just like the
    underlying ContextVar.
    """

    def __init__(self, debug_name: str, default: T):
        """The name passed in here is only for debugging purposes, as per the
        documentation for ContextVar.
        """
        self._contextvar = cv.ContextVar(debug_name, default=default)

    def set(self, value: T) -> ty.ContextManager[T]:
        return stack_context(self._contextvar, value)

    def __call__(self) -> T:
        return self._contextvar.get()
