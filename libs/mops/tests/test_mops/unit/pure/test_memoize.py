import pytest

from thds.mops.pure import pipeline_id_mask, pipeline_id_mask_from_docstr
from thds.mops.pure.core.memo import make_function_memospace
from thds.mops.pure.core.pipeline_id_mask import extract_mask_from_docstr, get_pipeline_id_mask


def fx(a: int) -> float:
    return float(a)


FX_NAME = "tests.test_mops.unit.pure.test_memoize:fx"


def test_that_set_function_pipeline_id_contextmanager_works():
    std = make_function_memospace("adls://foo/bar", fx)
    assert std.startswith("adls://foo/bar/")
    assert std.endswith(FX_NAME)
    assert "FOOPIPE" not in std

    with pipeline_id_mask("FOOPIPE") as visible:
        assert visible
        with pipeline_id_mask("NOT_USED") as visible:
            assert not visible
            assert f"adls://foo/bar/FOOPIPE/{FX_NAME}" == make_function_memospace("adls://foo/bar", fx)


def test_that_empty_pipeline_id_has_no_masking_effect():
    with pipeline_id_mask("") as visible:
        assert visible
        with pipeline_id_mask("YAHOO") as visible:
            assert visible
            assert f"adls://foo/bar/YAHOO/{FX_NAME}" == make_function_memospace("adls://foo/bar", fx)


def in_docstring():
    """
    pipeline-id-mask: WOO
    """
    pass


def test_extract_pipeline_id_mask_from_docstr():
    assert "WOO" == extract_mask_from_docstr(in_docstring)


def test_construct_pipeline_id_mask_from_func():
    with pipeline_id_mask_from_docstr(in_docstring) as visible:
        assert visible
        assert "WOO" == get_pipeline_id_mask()


def test_extract_failure_is_not_an_option():
    def no_key():
        """stuff but no pipeline id mask"""

    with pytest.raises(ValueError, match="Cannot extract pipeline-id-mask"):
        extract_mask_from_docstr(no_key)
    with pytest.raises(ValueError, match="non-empty docstring"):
        extract_mask_from_docstr(fx)

    def empty():
        """
        pipeline-id-mask:
        """

    with pytest.raises(ValueError, match="pipeline-id-mask is present but empty"):
        extract_mask_from_docstr(empty)
