from thds.mops.pure.core.output_naming import MEMO_URI_COMPONENTS

from ._util import adls_shim


@adls_shim
def mul(a: int, b: float = 4.2) -> float:
    components = MEMO_URI_COMPONENTS()
    assert components is not None
    assert components.args_hash == "SplitJarBlame.YZ6G93_dqAf2i9EB71jA0aQunEZOCFeOjXpd2gs", components
    return a * b


def test_memoization_of_args_kwargs_is_stable_across_different_looking_call_signatures():
    assert 16.8 == mul(4, 4.2)
    assert 16.8 == mul(4, b=4.2)
    assert 16.8 == mul(a=4, b=4.2)
    assert 16.8 == mul(b=4.2, a=4)
    assert 16.8 == mul(a=4)
