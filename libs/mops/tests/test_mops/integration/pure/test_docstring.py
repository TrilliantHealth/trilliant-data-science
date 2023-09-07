from ._util import adls_shell


@adls_shell
def func_with_docstring(a: int, b: int) -> int:
    """has a docstring with a version.

    mops-function-version: 1.3
    """
    return a - b


def test_can_access_docstring_and_mops_version():
    assert 1 == func_with_docstring(3, 2)
