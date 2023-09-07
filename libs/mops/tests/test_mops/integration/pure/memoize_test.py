"""Test that the function_pipeline_id decorator works when actually
calling a wrapped function at runtime.

"""
from typing import Sequence

import pytest

from thds.adls import defaults
from thds.mops.pure import AdlsPickleRunner, pipeline_id_mask, use_runner


class PipelineId(Exception):
    pass


def _intercept_pipeline_id(args: Sequence[str]) -> None:
    pickle_runner, memo_uri, pipeline_id = args
    raise PipelineId(pipeline_id)


# pipeline_id_mask must be applied 'outside' the use_runner
# decorator, as it works at call time, and its code must run before
# the underlying Runner implementation does.
@pipeline_id_mask("test/static-forever")
@use_runner(AdlsPickleRunner(_intercept_pipeline_id, defaults.env_root_uri("dev")))
def fx(a: int) -> float:
    return float(a) + 0.2


def test_pipeline_id_mask_sets_default():
    with pytest.raises(PipelineId, match="test/static-forever"):
        fx(4)


# application using non-decorator function call works
gx = pipeline_id_mask("test/override-static-forever")(fx)


def test_pipeline_id_mask_overrides_default():
    with pytest.raises(PipelineId, match="test/override-static-forever"):
        gx(4)


# we can even decorate a different function that calls the
# underlying function and it will work.
@pipeline_id_mask("test/override-override")
def hx(a: int):
    return pipeline_id_mask("test/SHOULD_NEVER_APPEAR_IN_ADLS")(gx)(a)


def test_pipeline_id_mask_multi_override():
    with pytest.raises(PipelineId, match="test/override-override"):
        hx(4)
