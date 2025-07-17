import typing as ty

R = ty.TypeVar("R")


class PFuture(ty.Protocol[R]):
    """
    A Protocol defining the behavior of a future-like object.

    This defines an interface for an object that acts as a placeholder
    for a result that will be available later. It is structurally
    compatible with concurrent.futures.Future but omits cancellation.
    """

    def running(self) -> bool:
        """Return True if the future is currently executing."""
        ...

    def done(self) -> bool:
        """Return True if the future is done (finished)."""
        ...

    def result(self, timeout: ty.Optional[float] = None) -> R:
        """
        Return the result of the work item.

        If the work item raised an exception, this method raises the same
        exception. If the timeout is reached, it raises TimeoutError.
        """
        ...

    def exception(self, timeout: ty.Optional[float] = None) -> ty.Optional[BaseException]:
        """
        Return the exception raised by the work item.

        Returns None if the work item completed without raising.
        If the timeout is reached, it raises TimeoutError.
        """
        ...

    def add_done_callback(self, fn: ty.Callable[["PFuture[R]"], None]) -> None:
        """
        Attaches a callable that will be called when the future is done.

        The callable will be called with the future object as its only
        argument.
        """
        ...
