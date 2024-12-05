import os
import typing as ty
from contextlib import closing
from pathlib import Path
from sqlite3 import connect
from timeit import default_timer

from thds.core import log, types

from .meta import get_indexes, get_tables, pydd

logger = log.getLogger(__name__)


def merge_databases(
    filenames: ty.Iterable[types.StrOrPath],
    table_names: ty.Collection[str] = tuple(),
    *,
    replace: bool = False,
    copy_indexes: bool = True,
) -> Path:
    """Merges the listed tables, if found in the other filenames,
    into the first database in the list. If no table names are listed,
    we will instead merge _all_ tables found in _every_ database into the first.

    If the table already exists in the destination, it will not be altered.
    If the table does not, it will be created using the SQL CREATE TABLE statement
    found in the first database where it is encountered.

    This mutates the first database in the list! If you don't want it
    mutated, make a copy of the file first, or start with an empty database file!

    Allows SQL injection via the table names - don't use this on untrusted inputs.

    By default, also copies indexes associated with the tables where an index with the
    same name does not already exist in the destination table.  You can disable this
    wholesale and then create/copy the specific indexes that you want after the fact.
    """
    _or_replace_ = "OR REPLACE" if replace else ""
    filenames = iter(filenames)
    first_filename = next(filenames)
    logger.info(f"Connecting to {first_filename}")
    conn = connect(first_filename)
    destination_tables = get_tables(conn)

    merge_start = default_timer()
    with closing(conn.cursor()) as cursor:
        cursor.execute("pragma synchronous = off;")
        cursor.execute("pragma journal_mode = off;")
        for filename in filenames:
            pydd(Path(filename))
            start = default_timer()
            to_merge = "to_merge"  # just a local/temporary constant alias
            logger.info(f"Merging {filename} into {first_filename}")
            cursor.execute(f"ATTACH '{os.fspath(filename)}' AS " + to_merge)

            attached_tables = get_tables(conn, schema_name=to_merge)
            cursor.execute("BEGIN")
            for table_name in table_names or attached_tables.keys():
                if table_name not in attached_tables:
                    continue  # table doesn't exist in the database to merge, so we skip it!
                if table_name not in destination_tables:
                    cursor.execute(attached_tables[table_name])  # create the table in the destination
                    destination_tables = get_tables(conn)  # refresh tables dict
                cursor.execute(
                    f"INSERT {_or_replace_} INTO {table_name} SELECT * FROM {to_merge}.[{table_name}]"
                )
                if copy_indexes:
                    dest_indexes = get_indexes(table_name, conn)
                    for idx_name, index_sql in get_indexes(
                        table_name, conn, schema_name=to_merge
                    ).items():
                        if idx_name not in dest_indexes:
                            cursor.execute(index_sql)
            logger.info(
                f"Committing merge of {filename} into {first_filename} after {default_timer() - start:.2f}s"
            )
            conn.commit()  # without a commit, DETACH DATABASE will error with Database is locked.
            # https://stackoverflow.com/questions/56243770/sqlite3-detach-database-produces-database-is-locked-error
            cursor.execute(f"DETACH DATABASE {to_merge}")

    logger.info(f"Merge complete after {default_timer() - merge_start:.2f}s")
    conn.close()
    return Path(first_filename)
