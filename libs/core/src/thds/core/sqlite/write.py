import typing as ty
from sqlite3 import Connection

from thds.core import generators, log

logger = log.getLogger(__name__)


def _run_batch_and_isolate_failures(cursor, query: str, batch: ty.List[tuple]):
    assert cursor, "cursor must exist"
    assert query, "query must be non-empty"
    if not batch:
        return
    try:
        cursor.executemany(query, batch)
    except Exception:
        if len(batch) >= 2:
            _run_batch_and_isolate_failures(cursor, query, batch[: len(batch) // 2])
            _run_batch_and_isolate_failures(cursor, query, batch[len(batch) // 2 :])
        else:
            bad_data = ""
            for i, col in enumerate(batch[0], 1):
                bad_data += f"COL {i}: {col}\n      {type(col)}\n"
            logger.exception(
                f"Failed during insertion; ***QUERY***\n{query} \n***FAILED-DATA***\n{bad_data}"
            )
        raise


def make_mapping_writer(
    conn: Connection,
    table_name: str,
    *,
    batch_size: int = 1000,
    replace: bool = False,
) -> ty.Generator[None, ty.Mapping[str, ty.Any], str]:
    """Return a generator that accepts mappings via send() and writes them in batches
    to the database. The generator itself yields nothing, but will return the table_name
    once it has been close()d.

    The reason for having a generator do this rather than a function that consumes an
    iterator is that the former is fundamentally more flexible - a 'power user' could
    consume multiple iterators and write to multiple of these generator writers in
    parallel without need for threading.
    """

    def make_query(first_row) -> str:
        columns = ",\n    ".join(row.keys())
        # Create a list of placeholders
        placeholders = ", ".join(["?"] * len(row))

        rpl = "OR REPLACE" if replace else ""
        # Construct the SQL query for the batch
        return f"INSERT {rpl} INTO {table_name} (\n    {columns}\n) VALUES ({placeholders})"

    query = ""
    cursor = None
    batch = list()

    try:
        while True:
            row = yield
            if not query:
                query = make_query(row)
                cursor = conn.cursor()

            batch.append(tuple(row.values()))

            if len(batch) >= batch_size:
                _run_batch_and_isolate_failures(cursor, query, batch)
                batch = list()

    except GeneratorExit:
        if not query:
            # we never got any rows
            logger.warning(f"No rows to write into table '{table_name}'")
            return ""

        # Insert any remaining data in the last batch
        _run_batch_and_isolate_failures(cursor, query, batch)
        # Commit the changes to the database
        conn.commit()
        return table_name
    finally:
        if cursor:
            cursor.close()


def write_mappings(
    conn: Connection,
    table_name: str,
    rows: ty.Iterable[ty.Mapping[str, ty.Any]],
    *,
    batch_size: int = 1000,
    replace: bool = False,
) -> None:
    generators.iterator_sender(
        make_mapping_writer(conn, table_name, batch_size=batch_size, replace=replace), rows
    )
