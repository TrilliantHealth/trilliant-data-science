# import ast

from ...conftest import ReferenceDataTestCase  # line_diff

# from thds.tabularasa.schema.compilation.io import ast_eq


def test_compile_attrs(test_case_with_compiled_attrs_sqlite: ReferenceDataTestCase):
    test_case = test_case_with_compiled_attrs_sqlite
    assert test_case.schema is not None
    assert test_case.attrs_sqlite_source is not None

    # generated_ast = ast.parse(test_case.attrs_sqlite_source)
    # TODO:
    # expected_ast = ast.parse(test_case.expected_attrs_sqlite_source)
    # assert ast_eq(
    #     generated_ast, expected_ast
    # ), f"sources differ:\n{line_diff(test_case.attrs_source, test_case.expected_attrs_source)}"
