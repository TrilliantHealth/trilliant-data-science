import contextlib
import typing as ty


@contextlib.contextmanager
def catch(allow: ty.Callable[[Exception], bool]) -> ty.Iterator:
    """try-except but flexible. Catch only Exceptions matching the filter.

    Useful for libraries like azure where all the Exceptions have the
    same type.
    """
    try:
        yield
    except Exception as e:
        if not allow(e):
            raise
