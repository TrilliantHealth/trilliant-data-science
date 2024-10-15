import pickle
import textwrap

from thds.mops.pure.pickling._pickle import read_partial_pickle


def test_can_unpickle_bytes_that_are_not_just_pickle():
    test_data_str = textwrap.dedent(
        """
    foo=bar
    bug=bear
    joy=world
    """
    ).strip("\n")

    test_data_bytes = test_data_str.encode("utf-8") + pickle.dumps(dict(a=1, b=2, c=3))

    text_bytes, unpickled = read_partial_pickle(test_data_bytes)
    assert dict(a=1, b=2, c=3) == unpickled
    assert text_bytes.decode("utf-8") == test_data_str
