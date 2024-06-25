# this works only if you have something to map the tuples to an attrs class.
# it also does not currently offer any parallelism.
import typing as ty

from thds.core import progress
from thds.core.sqlite import DbAndTable, TableSource, table_source

T = ty.TypeVar("T")


def tuples_to_table_source(
    data: ty.Iterable[tuple],
    upsert_many: ty.Callable[[ty.Iterable[T]], DbAndTable],
    func: ty.Callable[[tuple], ty.Optional[T]],
) -> TableSource:
    """
    Converts an iterable of tuples to a TableSource.
    :param data: An iterable of tuples.
    :param upsert_many: A function that accepts an iterable of T and returns a DbAndTable.
    :param func: A function that processes each tuple and returns an optional T.
    :return: The resulting table source after upserting data.
    """
    return table_source(*upsert_many(progress.report(filter(None, map(func, data)))))
