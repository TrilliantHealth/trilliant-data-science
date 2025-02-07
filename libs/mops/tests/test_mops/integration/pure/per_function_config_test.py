from pathlib import Path

from thds.core import config
from thds.mops.pure.core import uris
from thds.mops.pure.core.memo.function_memospace import (
    make_function_memospace,
    make_unique_name_including_docstring_key,
)
from thds.mops.pure.pickling._pickle import read_metadata_and_object
from thds.mops.pure.runner.local import invoke_via_shim_or_return_memoized

from ...config import TEST_TMP_URI
from ._util import _subprocess_remote, runner


def mul(a: int, b: float) -> float:
    return a * b


def broken_mul(a: int, b: float) -> float:
    assert "This should never get called and is also broken"
    return a / b


_NO_REDIRECT = lambda f, _args, _kwargs: f  # noqa: E731

_RESULT_PATH = Path(__file__).parent / "reuse_memoized_via_config.pickle"
assert _RESULT_PATH.exists(), f"Need to create the file {_RESULT_PATH} first."


def test_reuse_memoized_via_config():
    func_uri = (
        f"{TEST_TMP_URI}mops2-mpf/test/some-pipeline-id"
        f"/{make_unique_name_including_docstring_key(mul)}"
    )

    # setting up the 'memoized' result
    bs = uris.lookup_blob_store(func_uri)
    # this memo key is also testing stablity of our hash function
    result_uri = bs.join(func_uri, "SplitJarBlame.YZ6G93_dqAf2i9EB71jA0aQunEZOCFeOjXpd2gs", "result")
    if not bs.exists(result_uri):
        bs.putfile(_RESULT_PATH, result_uri)
    # end setup

    def shim_builder(f, _args, _kwargs):
        return _subprocess_remote

    def noop(_, b):
        pass

    args, kwargs = (4,), dict(b=4.2)  # type: ignore
    assert 16.8 == invoke_via_shim_or_return_memoized(
        runner._serialize_args_kwargs,
        runner._serialize_invocation,  # mul
        shim_builder,
        read_metadata_and_object,
    )(True, func_uri, noop, args, kwargs)

    assert 16.8 == invoke_via_shim_or_return_memoized(
        runner._serialize_args_kwargs,
        runner._serialize_invocation,
        shim_builder,
        read_metadata_and_object,
        # broken_mul,  # won't actually run broken_mul - will instead look up the results from mul
    )(True, func_uri, noop, args, kwargs)


def test_actual_config_is_used():
    val = "adls://foobar/quuxbaz/blah"
    config_name = f"mops.memo.{make_unique_name_including_docstring_key(mul)}.memospace"
    config.set_global_defaults({config_name: ""})
    config_item = config.config_by_name(config_name)

    with config_item.set_local(val):
        assert val == make_function_memospace(TEST_TMP_URI, mul)

    assert val != make_function_memospace(TEST_TMP_URI, mul)
