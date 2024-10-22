import typing as ty
from sqlite3 import Connection, Row


def matching_where(to_match_colnames: ty.Iterable[str]) -> str:
    """Creates a where clause for these column names, with named @{col} placeholders for each column."""
    qs = " AND ".join(f"{k} = @{k}" for k in to_match_colnames)
    return f"WHERE {qs}" if qs else ""


def matching_select(
    table_name: str,
    conn: Connection,
    to_match: ty.Mapping[str, ty.Any],
    columns: ty.Sequence[str] = tuple(),
) -> ty.Iterator[ty.Mapping[str, ty.Any]]:
    """Get a single row from a table by key.

    This is susceptible to SQL injection because the keys are
    formatted directly. Do _not_ give external users the ability to
    call this function directly and specify any of its keys.
    """
    cols = ", ".join(columns) if columns else "*"

    qs = " AND ".join(f"{k} = ?" for k in to_match.keys())
    where = f"WHERE {qs}" if qs else ""
    # because we control the whole query, we're matching on the 'dumb' ? placeholder.

    old_row_factory = conn.row_factory
    conn.row_factory = Row  # this is an optimized approach to getting 'mappings' (with key names)
    for row in conn.execute(f"SELECT {cols} FROM {table_name} {where}", tuple(to_match.values())):
        yield row
    conn.row_factory = old_row_factory


matching = matching_select  # alias


def partition(
    n: int, i: int, columns: ty.Optional[ty.Union[str, ty.Collection[str]]] = None
) -> ty.Dict[str, int]:
    """Can (surprisingly) be used directly with matching().

    i should be zero-indexed, whereas N is a count (natural number).

    columns is an optional parameter to specify column(s) to partition on
    if no columns are specified partitioning will be based on rowid
    """
    assert 0 <= i < n
    if not columns:
        return {f"rowid % {n}": i}
    hash_cols = columns if isinstance(columns, str) else ", ".join(columns)
    # when SQLite uses this in a WHERE clause, as "hash(foo, bar) % 5 = 3",
    # it hashes the _values_ in the row for those columns.
    # Note that if we do have to fall back to this, the partitioning will be
    # quite a bit slower because of the necessity of calling back into Python.
    return {f"_pyhash_values({hash_cols}) % {n}": i}


def maybe(
    table_name: str, conn: Connection, to_match: ty.Mapping[str, ty.Any]
) -> ty.Optional[ty.Mapping[str, ty.Any]]:
    """Get a single row, if it exists, from a table by key"""
    results = list(matching(table_name, conn, to_match))
    assert len(results) == 0 or len(results) == 1
    return results[0] if results else None
