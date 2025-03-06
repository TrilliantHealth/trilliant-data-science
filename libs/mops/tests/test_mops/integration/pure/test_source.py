# this is where we 'integration test' the 'source' concept using full
# MemoizingPicklingRunner.
import typing as ty
from random import randint

from thds.core.source import Source, from_file
from thds.mops import tempdir
from thds.mops.pure import memoize_in, pipeline_id_mask

from ...config import TEST_TMP_URI


def a_function_that_combines_two_sources(both: ty.Tuple[Source, Source]) -> ty.Dict[str, Source]:
    """pipeline-id-mask: test/mops/combine-sources"""
    # takes a tuple just so we can see that recursive serialization works.
    output_file = tempdir() / "by_your_sources_combined.txt"
    with open(output_file, "w") as f:
        for source in both:
            with open(source) as sf:
                f.write(sf.read())

    return dict(yes=from_file(output_file))


def test_that_sources_get_transferred_both_directions_via_local_hashrefs(temp_file, caplog):
    caplog.set_level(10)
    src_a = from_file(temp_file("Captain"))
    src_b = from_file(temp_file(" Planet"))

    mask = f"test/mops-combine-sources/{randint(0, 99999)}"
    with pipeline_id_mask(mask):
        cp = memoize_in(TEST_TMP_URI)(a_function_that_combines_two_sources)((src_a, src_b))["yes"]

    uri_root = TEST_TMP_URI[:7]  # bit of a hack here. we basically just care about the scheme matching.
    assert cp.uri.endswith("by_your_sources_combined.txt")
    assert cp.uri.startswith(uri_root)
    assert cp.cached_path and cp.cached_path.exists()
    assert open(cp).read() == "Captain Planet"
