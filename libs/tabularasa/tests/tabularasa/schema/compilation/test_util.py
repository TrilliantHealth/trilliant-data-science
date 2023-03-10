import pytest

from thds.tabularasa.schema.compilation.util import sorted_class_names_for_import


@pytest.mark.parametrize(
    "names,expected",
    [
        (["FOO", "Bar", "Baz"], ["FOO", "Bar", "Baz"]),
        (["BIG", "Medium", "SMALL"], ["BIG", "SMALL", "Medium"]),
        (
            ["CONSTANT", "GLOBAL", "OTHER_GLOBAL", "Class"],
            ["CONSTANT", "GLOBAL", "OTHER_GLOBAL", "Class"],
        ),
        (["Class1", "Class2", "OTHER"], ["OTHER", "Class1", "Class2"]),
    ],
)
def test_sorted_class_names_for_import(names, expected):
    assert sorted_class_names_for_import(names) == expected
