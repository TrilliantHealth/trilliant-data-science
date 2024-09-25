from functools import partial

from thds.mops.pure.core.memo.unique_name_for_function import (
    make_unique_name_including_docstring_key,
    parse_unique_name,
)


def foobar():
    """Some random stuff.

    function-logic-key: 2.3.7

    more stuff.
    """


MODULE_BASE = "tests.test_mops.unit.pure.test_unique_name_for_function"


def test_extract_name_and_version_for_function():
    assert f"{MODULE_BASE}--foobar@2.3.7" == make_unique_name_including_docstring_key(foobar)


def barbaz():
    """foo foo foo

    function-logic-key: 2023-03-13-foobar

    whatever.
    """


def test_key_can_be_arbitrary_string():
    assert f"{MODULE_BASE}--barbaz@2023-03-13-foobar" == make_unique_name_including_docstring_key(barbaz)


class CallableClass:
    """function-logic-key: 7.88"""

    def __call__(self, _bar):
        pass


def test_extract_name_and_version_for_classobject():
    assert f"{MODULE_BASE}--CallableClass@7.88" == make_unique_name_including_docstring_key(
        CallableClass()
    )


def yoyo(a, b):
    """function-logic-key: 3.1415"""
    return a * b


def test_extract_name_and_version_for_partial_func():
    assert f"{MODULE_BASE}--yoyo@3.1415" == make_unique_name_including_docstring_key(partial(yoyo, 1))


def test_parse_unique_name():
    module, name, flk = parse_unique_name(
        "tests.test_mops.unit.pure.test_unique_name_for_function--yoyo@3.1415"
    )
    assert module == MODULE_BASE
    assert name == "yoyo"
    assert flk == "3.1415"
