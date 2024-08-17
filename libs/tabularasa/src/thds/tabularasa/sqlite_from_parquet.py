from pathlib import Path

from thds.core import log, source
from thds.tabularasa.data_dependencies.sqlite import insert_table
from thds.tabularasa.loaders.sqlite_util import sqlite_connection
from thds.tabularasa.schema import load_schema
from thds.tabularasa.schema.metaschema import Table

logger = log.getLogger(__name__)


def load_table_from_schema(schema_path: Path, table_name: str) -> Table:
    return load_schema(None, str(schema_path)).tables[table_name]


def sqlite_from_parquet(
    schema_src: source.Source,
    table_name: str,
    parquet_src: source.Source,
    output_db_dir: Path,
    output_db_name: str,
) -> source.Source:
    parquet_path = parquet_src.path()
    sqlite_outfile = output_db_dir / output_db_name
    with sqlite_connection(sqlite_outfile) as conn:
        insert_table(
            conn,
            load_table_from_schema(schema_src.path(), table_name),
            None,
            data_dir=str(parquet_path.parent),
            filename=str(parquet_path.name),
        )
    logger.info("Done inserting parquet into sqlite.")
    return source.from_file(sqlite_outfile)
