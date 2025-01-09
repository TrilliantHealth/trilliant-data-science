from thds.adls import defaults
from thds.core import config
from thds.mops.pure.core.memo.function_memospace import (
    make_function_memospace,
    make_unique_name_including_docstring_key,
)
from thds.mops.pure.pickling._pickle import read_metadata_and_object
from thds.mops.pure.runner.local import invoke_via_shell_or_return_memoized

from ...config import TEST_TMP_URI
from ._util import _subprocess_remote, runner


def mul(a: int, b: float) -> float:
    return a * b


def broken_mul(a: int, b: float) -> float:
    assert "This should never get called and is also broken"
    return a / b


_NO_REDIRECT = lambda f, _args, _kwargs: f  # noqa: E731


def test_reuse_memoized_via_config():
    func_uri = (
        f"{TEST_TMP_URI}mops2-mpf/test/some-pipeline-id"
        f"/{make_unique_name_including_docstring_key(mul)}"
    )

    def shell_builder(f, _args, _kwargs):
        return _subprocess_remote

    def noop(_, b):
        pass

    args, kwargs = (4,), dict(b=4.2)  # type: ignore
    assert 16.8 == invoke_via_shell_or_return_memoized(
        runner._serialize_args_kwargs,
        runner._serialize_invocation,  # mul
        shell_builder,
        read_metadata_and_object,
    )(True, func_uri, noop, args, kwargs)

    assert 16.8 == invoke_via_shell_or_return_memoized(
        runner._serialize_args_kwargs,
        runner._serialize_invocation,
        shell_builder,
        read_metadata_and_object,
        # broken_mul,  # won't actually run broken_mul - will instead look up the results from mul
    )(True, func_uri, noop, args, kwargs)


def test_actual_config_is_used():
    val = "adls://foobar/quuxbaz/blah"
    config_name = f"mops.memo.{make_unique_name_including_docstring_key(mul)}.memospace"
    config.set_global_defaults({config_name: ""})
    config_item = config.config_by_name(config_name)

    with config_item.set_local(val):
        assert val == make_function_memospace(defaults.env_root_uri("dev"), mul)

    assert val != make_function_memospace(defaults.env_root_uri("dev"), mul)
