"""Utilities for working with concurrency in Python."""
import contextvars
import typing as ty
from concurrent.futures import ThreadPoolExecutor


def copy_context():
    """The basic implementation you want if you want to copy the current ContextVar
    context to a new thread. https://docs.python.org/3.10/library/contextvars.html

    Makes a copy of the current context, and closes over that copy with a callable that
    must then be called inside the new thread (or process, if your context is picklable).

    It is disappointing that Python does not do this for you by default, since it is quite
    common to want to do, extremely cheap, and is much easier to write the code to
    manually override in the rare cases where it's the wrong idea, than it is to make sure
    to put this in every single place you want it to happen. Which is probably why asyncio
    _does_ do this by default for green/async coroutines...

    """
    context = contextvars.copy_context()

    def copy_context_initializer():
        for var, value in context.items():
            var.set(value)

    return copy_context_initializer


class ContextfulInit(ty.TypedDict):
    """A dictionary corresponding to the initializer API expected by concurrent.futures.Executor"""

    initializer: ty.Callable[[], None]


def initcontext() -> ContextfulInit:
    """Returns a dictionary corresponding to the API expected by concurrent.futures.Executor,

    so that you can do `ThreadPoolExecutor(**initcontext())` to get a ThreadPoolExecutor that
    copies the current context to the new thread.
    """
    return dict(initializer=copy_context())


def contextful_threadpool_executor(
    max_workers: ty.Optional[int] = None,
) -> ty.ContextManager[ThreadPoolExecutor]:
    """
    Return a ThreadPoolExecutor that copies the current context to the new thread.

    You don't need to use this directly.
    """
    return ThreadPoolExecutor(
        max_workers=max_workers,
        thread_name_prefix="contextful_threadpool_executor",
        **initcontext(),
    )
