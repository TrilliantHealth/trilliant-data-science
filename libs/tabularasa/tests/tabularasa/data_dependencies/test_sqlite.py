from thds.tabularasa.data_dependencies.sqlite import table_exists, table_populated


def test_populate_sqlite_db(test_case_with_sqlite_db):
    if test_case_with_sqlite_db.schema.build_options.package_data_dir:
        conn = test_case_with_sqlite_db.sqlite_db_conn
        for table in test_case_with_sqlite_db.schema.build_time_package_tables:
            _assert_table_correct(conn, table)


def _assert_table_correct(conn, table):
    if table.has_indexes:
        assert table_exists(conn, table), f"Expected table {table.name!r} not found in sqlite database"
        assert table_populated(
            conn, table
        ), f"Columns of database table {table.name!r} not aligned with table object"
    else:
        assert not table_exists(conn, table), f"Unexpected table {table.name!r} found in sqlite database"
