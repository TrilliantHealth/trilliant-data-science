from pathlib import Path
from typing import Dict, List

import pytest

from thds.mops.pure import memoize_in, pipeline_id_mask, pipeline_id_mask_from_docstr
from thds.mops.pure.core.memo import make_function_memospace, parse_memo_uri
from thds.mops.pure.core.pipeline_id_mask import extract_mask_from_docstr, get_pipeline_id_mask


def fx(a: int) -> float:
    return float(a)


FX_NAME = "tests.test_mops.unit.pure.test_memoize--fx"


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


def get_storage_root() -> str:
    script_dir = Path(__file__).parent.resolve()
    return f"file:///{script_dir}"


# use memoization for simple functions
memo = memoize_in(get_storage_root())


@memo
def add_numbers(a: int, b: int) -> int:
    return a + b


@memo
def multiply_numbers(a: int, b: int) -> int:
    return a * b


def process_numbers(numbers: List[Dict[str, int]]) -> List[Dict[str, int]]:
    for item in numbers:
        item["sum"] = add_numbers(item["A"], item["B"])
        item["product"] = multiply_numbers(item["A"], item["B"])
    return numbers


# Main function to run the pipeline
def run_pipeline() -> List[Dict[str, int]]:
    data: List[Dict[str, int]] = [{"A": 1, "B": 2}, {"A": 3, "B": 4}, {"A": 5, "B": 6}]
    return process_numbers(data)


def test_integration_local_filesystem() -> None:
    processed_data = run_pipeline()
    expected_data = [
        {"A": 1, "B": 2, "sum": 3, "product": 2},
        {"A": 3, "B": 4, "sum": 7, "product": 12},
        {"A": 5, "B": 6, "sum": 11, "product": 30},
    ]
    assert processed_data == expected_data


def test_parse_memo_uri():
    memo_parts = parse_memo_uri(
        "adls://foo/bar/mops2-mpf/FOOPIPE/PIPEA/PIPEB/tests.test_mops.unit.pure.test_memoize--fx@flk45/PurseHowCorgi-89723098273409283742938742"
    )
    assert memo_parts.memospace == "adls://foo/bar/mops2-mpf"
    assert memo_parts.pipeline_id == "FOOPIPE/PIPEA/PIPEB"
    assert memo_parts.function_module == "tests.test_mops.unit.pure.test_memoize"
    assert memo_parts.function_name == "fx"
    assert memo_parts.function_logic_key == "flk45"
    assert memo_parts.args_hash == "PurseHowCorgi-89723098273409283742938742"
