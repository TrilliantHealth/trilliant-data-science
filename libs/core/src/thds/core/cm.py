"""A keep-alive wrapper for context managers. Let's say you've got a thread pool executor
that you've created, and you want to be able to pass it to multiple users that expect to
'enter' the thread pool themselves, using a `with` statement. But you don't want the
threads to be destroyed after the first use; you want to open the context yourself, but
still pass the expected context manager to the users. This is a way to do that.
"""

import contextlib
import typing as ty

T = ty.TypeVar("T")


class _AlreadyEnteredContext(ty.ContextManager[T]):
    def __init__(self, entered_context: T):
        self.entered_context = entered_context

    def __enter__(self) -> T:
        # No-op enter; just return the underlying thing
        return self.entered_context

    def __exit__(self, exc_type, exc_value, traceback):  # type: ignore
        pass  # No-op exit


@contextlib.contextmanager
def keep_context(context_manager: ty.ContextManager[T]) -> ty.Iterator[ty.ContextManager[T]]:
    with context_manager as entered_context:
        yield _AlreadyEnteredContext(entered_context)
