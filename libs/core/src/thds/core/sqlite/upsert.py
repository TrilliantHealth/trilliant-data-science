import textwrap
import typing as ty
from functools import lru_cache
from sqlite3 import Connection

from thds.core import generators, log

from .meta import primary_key_cols
from .write import run_batch_and_isolate_failures

logger = log.getLogger(__name__)


def _make_upsert_writer(
    conn: Connection,
    table_name: str,
    batch_size: int = 1000,
    max_sql_stmt_cache_size: int = 1000,
) -> ty.Generator[None, ty.Mapping[str, ty.Any], str]:
    """Upserts in SQLite are a bit... under-featured. You simply cannot ask SQLite in a
    generic way to write the key-value pairs you've provided for a row but not to overwrite
    any key-value pairs you didn't provide with whatever the default value is (often NULL).

    In fact, the docs normally suggest doing a SELECT first to see if the row exists...

    What you can do instead is write a query that detects the conflict and follows it up
    with UPDATE for the specific key-value pairs you provided. On top of that, we can then
    batch any immediately-following rows that fit the exact same set of keys to be
    written, and finally, we can commit all of the queries at the end.  This won't be as
    fast as a true bulk insert, since there's meaningful Python running for every row
    (converting dict keys into a tuple) and a check against the previous keyset.

    Your perfomance will be better if you are able to make sure that your iterator of rows
    provides rows with the same keys in the same order in batches, so that we can do as
    little SQL formatting as possible and execute larger batches with executemany.
    """

    primary_keys = primary_key_cols(table_name, conn)
    on_conflict_pkeys = ",".join(primary_keys)
    # https://stackoverflow.com/questions/66904339/sqlite-on-conflict-two-or-more-columns

    @lru_cache(maxsize=max_sql_stmt_cache_size)
    def make_upsert_query(row_keys: ty.Tuple[str, ...]) -> str:
        """Prefer sorting your row keys so that you get overlap here."""
        insert_columns = ",\n    ".join(row_keys)
        placeholders = ", ".join(["?"] * len(row_keys))

        non_pkey_rows = [col for col in row_keys if col not in primary_keys]

        # Construct the SQL query for the batch
        return textwrap.dedent(
            f"""
            INSERT INTO {table_name} ({insert_columns})
                VALUES ({placeholders})
                ON CONFLICT ({on_conflict_pkeys}) DO UPDATE SET
                    {", ".join(f"{col}=excluded.{col}" for col in non_pkey_rows)};
            """
        )

    cursor = None
    batch: ty.List[ty.Tuple[ty.Any, ...]] = list()
    query = ""
    current_keyset: ty.Tuple[str, ...] = tuple()

    try:
        row = yield

        cursor = conn.cursor()
        # don't create the cursor til we receive our first actual row.

        while True:
            keyset = tuple(row)
            # Based on some benchmarking, and on the (imagined) most likely actual use
            # cases, we're choosing not to sort your keys for you - if your rows have
            # different key orders, we won't be able to make batches as large, and
            # performance will decrease.  It seems likely that in most real-world use
            # cases, the code for generating the rows will have somewhat determinstic key
            # ordering. If this isn't the case, you're welcome to reorder the keys as you
            # see fit - perhaps it will make your code faster, but my guess is that in
            # many cases, by taking on that extra Python overhead, you'll end up slower
            # overall. Either way, the power is in your hands and we're leaving this code
            # uncomplicated by that detail.
            if keyset != current_keyset or len(batch) >= batch_size:
                # send current batch:
                run_batch_and_isolate_failures(cursor, query, batch)

                batch = list()
                query = make_upsert_query(keyset)
                current_keyset = keyset

            batch.append(tuple(row.values()))
            row = yield

    except GeneratorExit:
        if not query:
            # we never got any rows
            logger.warning(f"No rows to upsert into table '{table_name}'")
            return ""

        # Insert any remaining data in the last batch
        run_batch_and_isolate_failures(cursor, query, batch)
        # Commit the changes to the database
        conn.commit()
        return table_name
    finally:
        if cursor:
            cursor.close()


def mappings(
    conn: Connection,
    table_name: str,
    rows: ty.Iterable[ty.Mapping[str, ty.Any]],
    *,
    batch_size: int = 1000,
) -> None:
    """Write rows to a table, upserting on the primary keys. Will not overwrite existing values that are not contained within the provided mappings.

    Note that core.sqlite.write.write_mappings is likely to be significantly faster than
    this if your rows have homogeneous keys (e.g. if you're writing the full row for each
    mapping), because this routine needs to generate a specific SQL statement for every
    unique combination of keys it sees (and so needs to examine the keys for every row).
    """
    generators.iterator_sender(_make_upsert_writer(conn, table_name, batch_size=batch_size), rows)
