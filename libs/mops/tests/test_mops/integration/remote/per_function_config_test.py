from thds.mops.config import get_memo_storage_root, set_config
from thds.mops.remote._memoize import make_function_memospace, make_unique_name_including_docstring_key
from thds.mops.remote.pickle_runner import _pickle_func_and_run_via_shell

from ._util import _subprocess_remote, runner


def mul(a: int, b: float) -> float:
    return a * b


def broken_mul(a: int, b: float) -> float:
    assert "This should never get called and is also broken"
    return a / b


def test_reuse_memoized_via_config():
    func_uri = (
        "adls://thdsscratch/tmp/mops/pipeline-pickled-functions-v1/some-pipeline-id"
        f"/{make_unique_name_including_docstring_key(mul)}"
    )

    args, kwargs = (4,), dict(b=4.2)  # type: ignore
    assert 16.8 == _pickle_func_and_run_via_shell(
        func_uri,
        runner._get_dumper,
        mul,
    )(_subprocess_remote, True, args, kwargs)

    assert 16.8 == _pickle_func_and_run_via_shell(
        func_uri,
        runner._get_dumper,
        broken_mul,  # won't actually run broken_mul - will instead look up the results from mul
    )(_subprocess_remote, True, args, kwargs)


def test_actual_config_is_used():
    val = "adls://foobar/quuxbaz/blah"
    with set_config("mops", "memo", make_unique_name_including_docstring_key(mul), "memospace")(val):
        assert val == make_function_memospace(get_memo_storage_root(), mul)

    assert val != make_function_memospace(get_memo_storage_root(), mul)
