import textwrap
import typing as ty
from functools import lru_cache
from sqlite3 import Connection

from thds.core import generators, log

from .meta import get_table_schema, primary_key_cols
from .read import matching_where
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

    We _tried_ doing an ON CONFLICT... DO UPDATE SET clause, but it turns out that
    does not work in circumstances described below. So we ended up with an approach
    that basically embeds the SELECT (so that this can be done in pure SQL rather than requiring
    Python logic to run for each row).

    By doing it this way, we can batch any immediately-following rows that fit the exact
    same set of keys to be written, and finally, we can commit all of the queries at the
    end.  This won't be as fast as a true bulk insert, since there's still meaningful
    Python running for every row (converting dict keys into a tuple) and a check against
    the previous keyset.

    Your perfomance will be better if you are able to make sure that your iterator of rows
    provides rows with the same keys in the same order in batches, so that we can do as
    little SQL formatting as possible and execute larger batches with executemany.
    """

    primary_keys = primary_key_cols(table_name, conn)
    where_matches_primary_keys = matching_where(primary_keys)
    all_column_names = tuple(get_table_schema(conn, table_name).keys())
    all_column_names_comma_str = ", ".join(all_column_names)

    # https://stackoverflow.com/questions/418898/upsert-not-insert-or-replace/4330694#comment65393759_7511635
    #
    # the above is the approach i'm taking now that I know that SQLite will (sadly) enforce a
    # not-null constraint _before_ it actually discovers that the row already exists and that
    # the ON CONFLICT clause would end up doing a simple UPDATE to an existing row.
    # This is more boilerplate-y and might be slower, too, because it requires a separate SELECT -
    # but in theory the database had to do that SELECT in order to check the ON CONFLICT clause anyway,
    # so maybe it's a wash?

    @lru_cache(maxsize=max_sql_stmt_cache_size)
    def make_upsert_query(colnames_for_partial_row: ty.Sequence[str]) -> str:
        """Makes a query with placeholders which are:

        - the values you provide for the row keys
        - the primary keys themselves, which must be provided in the same order as they are
          defined in the table schema.

        Prefer sorting your row keys so that you get overlap here.
        """
        colnames_or_placeholders = list()
        for col in all_column_names:
            if col in colnames_for_partial_row:
                colnames_or_placeholders.append(f"@{col}")  # insert/update the provided value
            else:
                colnames_or_placeholders.append(col)  # use the joined default value for an update

        # Construct the SQL query for the batch
        return textwrap.dedent(
            f"""
            INSERT OR REPLACE INTO {table_name} ({all_column_names_comma_str})
                SELECT {", ".join(colnames_or_placeholders)}
                FROM ( SELECT NULL )
                LEFT JOIN (
                    SELECT * from {table_name} {where_matches_primary_keys}
                )
            """
        )

    cursor = None
    batch: ty.List[ty.Mapping[str, ty.Any]] = list()
    query = ""
    current_keyset: ty.Tuple[str, ...] = tuple()

    try:
        row = yield
        cursor = conn.cursor()
        # don't create the cursor til we receive our first actual row.

        while True:
            keyset = tuple([col for col in all_column_names if col in row])
            if keyset != current_keyset or len(batch) >= batch_size:
                # send current batch:
                run_batch_and_isolate_failures(cursor, query, batch)

                batch = list()
                query = make_upsert_query(keyset)
                current_keyset = keyset

            batch.append(row)
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
