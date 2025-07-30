import pickle
import textwrap
from functools import partial

from thds.mops.pure.pickling._pickle import (
    Dumper,
    NestedFunctionWithLogicKeyPickler,
    _unpickle_with_callable,
    gimme_bytes,
    read_partial_pickle,
)


def test_can_unpickle_bytes_that_are_not_just_pickle():
    test_data_str = textwrap.dedent(
        """
    foo=bar
    bug=bear
    joy=world
    """
    ).strip("\n")

    test_data_bytes = test_data_str.encode("utf-8") + pickle.dumps(dict(a=1, b=2, c=3))

    text_bytes, first_pickle = read_partial_pickle(test_data_bytes)
    unpickled = pickle.loads(first_pickle)
    assert dict(a=1, b=2, c=3) == unpickled
    assert text_bytes.decode("utf-8") == test_data_str


def foobar_has_logic_key():
    """
    function-logic-key: a-crazy-key
    """
    return "bazzzz"


def calls_foobar(input: str, another_func) -> str:
    return input + another_func()


def test_pickle_contains_function_logic_key():
    pickle_bytes = gimme_bytes(
        Dumper(NestedFunctionWithLogicKeyPickler()),
        ("ya", partial(calls_foobar, "foo", foobar_has_logic_key)),
    )
    assert b"a-crazy-key" in pickle_bytes  # the logic key is in the pickle

    _, first_pickle = read_partial_pickle(pickle_bytes)
    obj = _unpickle_with_callable(first_pickle)
    assert obj[0] == "ya"
    assert obj[1]() == "foobazzzz"
