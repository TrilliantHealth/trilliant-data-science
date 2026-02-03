from typing import Optional

from thds.tabularasa.schema.compilation.sqlite import AUTOGEN_DISCLAIMER

from ...conftest import ReferenceDataTestCase, line_diff


def test_compile_sql(test_case_with_compiled_sqlite: ReferenceDataTestCase):
    def _assert_sql_eq(actual: Optional[str] = None, expected: Optional[str] = None):
        _actual = actual or ""
        _expected = f"-- {AUTOGEN_DISCLAIMER}\n\n" + (expected or "")
        actual_normalized = " ".join(_actual.split())
        expected_normalized = " ".join(_expected.split())
        assert actual_normalized == expected_normalized, (
            f"sources differ:\n{line_diff(_actual, _expected)}"
        )

    _assert_sql_eq(
        test_case_with_compiled_sqlite.sqlite_table_source,
        test_case_with_compiled_sqlite.expected_sqlite_table_source,
    )
    _assert_sql_eq(
        test_case_with_compiled_sqlite.sqlite_index_source,
        test_case_with_compiled_sqlite.expected_sqlite_index_source,
    )
