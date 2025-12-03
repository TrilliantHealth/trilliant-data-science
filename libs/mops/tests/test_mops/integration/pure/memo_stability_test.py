from thds.mops.pure.core.output_naming import pipeline_function_invocation_unique_key

from ._util import adls_shim


@adls_shim
def mul(a: int, b: float = 4.2) -> float:
    in_un_key = pipeline_function_invocation_unique_key()
    assert in_un_key
    pf, fa = in_un_key
    assert fa == "SplitJarBlame.YZ6G93_dqAf2i9EB71jA0aQunEZOCFeOjXpd2gs", in_un_key
    return a * b


def test_memoization_of_args_kwargs_is_stable_across_different_looking_call_signatures():
    assert 16.8 == mul(4, 4.2)
    assert 16.8 == mul(4, b=4.2)
    assert 16.8 == mul(a=4, b=4.2)
    assert 16.8 == mul(b=4.2, a=4)
    assert 16.8 == mul(a=4)
