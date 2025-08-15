# Register expensive "only if we actually need to invoke the function" work to be performed later
#
# this could be _any_ kind of work, but is only uploads as of initial abstraction.
# this basic idea was stolen from `pure.core.source` as a form of optimization for
# uploading Sources and their hashrefs.
import concurrent.futures
import typing as ty
from contextlib import contextmanager

from thds import core
from thds.core import config, refcount
from thds.core.stack_context import StackContext

_DEFERRED_INVOCATION_WORK: StackContext[
    ty.Optional[ty.Dict[ty.Hashable, ty.Callable[[], ty.Any]]]
] = StackContext("DEFERRED_INVOCATION_WORK", None)
_MAX_DEFERRED_WORK_THREADS = config.item("max_deferred_work_threads", default=50, parse=int)
_DEFERRED_WORK_THREADPOOL = refcount.Resource[concurrent.futures.ThreadPoolExecutor](
    lambda: concurrent.futures.ThreadPoolExecutor(
        max_workers=_MAX_DEFERRED_WORK_THREADS(), **core.concurrency.initcontext()
    )
)
logger = core.log.getLogger(__name__)


class NoDeferredWorkContext(Exception):
    """Raised when work is pushed with no open context"""


@contextmanager
def open_context() -> ty.Iterator[None]:
    """Enter this context before you begin serializing your invocation. When perform_all()
    is later called, any deferred work will be evaluated. The context should not be
    closed until after return from the Shim.

    The idea is that you'd call perform_all() inside your Shim which transfers
    execution to a remote environment, but _not_ call it if you're transferring execution
    to a local environment, as the upload will not be needed.
    """
    with _DEFERRED_INVOCATION_WORK.set(dict()):
        logger.debug("Opening deferred work context")
        yield
        logger.debug("Closing deferred work context")
        work_unperformed = _DEFERRED_INVOCATION_WORK()
        if work_unperformed:
            logger.debug(
                "some deferred work was not performed before context close: %s", work_unperformed
            )


def add(work_owner: str, work_id: ty.Hashable, work: ty.Callable[[], ty.Any]) -> None:
    """Add some work to an open context. The work will be performed when perform_all() is
    called. If there is no open context, perform the work immediately.

    The work_owner should usually be the module __name__, but if multiple things
    in a module need to add different types of tasks, then it can be anything
    that would further disambiguate.

    The work_id should be a unique id within the work_owner 'namespace'.
    """
    deferred_work = _DEFERRED_INVOCATION_WORK()
    if deferred_work is None:
        raise NoDeferredWorkContext("Deferred work can only be added when there is an open context.")
    else:
        logger.debug("Adding work %s to deferred work %s", (work_owner, work_id), id(deferred_work))
        deferred_work[(work_owner, work_id)] = work


def perform_all() -> None:
    """execute all the deferred work that has been added to the current context."""
    work_items = _DEFERRED_INVOCATION_WORK()
    if work_items:
        logger.info("Performing %s items of deferred work", len(work_items))
        with _DEFERRED_WORK_THREADPOOL.get() as thread_pool_executor:
            for key, _ in core.parallel.failfast(
                core.parallel.yield_all(dict(work_items).items(), executor_cm=thread_pool_executor)
            ):
                # consume iterator but don't keep results in memory.
                logger.debug("Popping deferred work %s from %s", key, id(work_items))
                work_items.pop(key)

        logger.debug("Done performing deferred work on %s", id(work_items))
        assert not work_items, f"Some deferred work was not performed! {work_items}"
