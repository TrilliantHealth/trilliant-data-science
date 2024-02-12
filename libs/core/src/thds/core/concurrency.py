import contextvars
import typing as ty
from concurrent.futures import ThreadPoolExecutor


def copy_context():
    context = contextvars.copy_context()

    def copy_context_initializer():
        for var, value in context.items():
            var.set(value)

    return copy_context_initializer


class ContextfulInit(ty.TypedDict):
    initializer: ty.Callable[[], None]


def initcontext() -> ContextfulInit:
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
