from thds.adls import defaults
from thds.mops.config import set_config
from thds.mops.pure.core.memo.function_memospace import (
    make_function_memospace,
    make_unique_name_including_docstring_key,
)
from thds.mops.pure.pickling.runner.orchestrator_side import _pickle_func_and_run_via_shell

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

    args, kwargs = (4,), dict(b=4.2)  # type: ignore
    assert 16.8 == _pickle_func_and_run_via_shell(
        func_uri,
        runner._get_stateful_dumper,
        mul,
    )(_subprocess_remote, True, _NO_REDIRECT, args, kwargs)

    assert 16.8 == _pickle_func_and_run_via_shell(
        func_uri,
        runner._get_stateful_dumper,
        broken_mul,  # won't actually run broken_mul - will instead look up the results from mul
    )(_subprocess_remote, True, _NO_REDIRECT, args, kwargs)


def test_actual_config_is_used():
    val = "adls://foobar/quuxbaz/blah"
    with set_config("mops", "memo", make_unique_name_including_docstring_key(mul), "memospace")(val):
        assert val == make_function_memospace(defaults.env_root_uri("dev"), mul)

    assert val != make_function_memospace(defaults.env_root_uri("dev"), mul)
