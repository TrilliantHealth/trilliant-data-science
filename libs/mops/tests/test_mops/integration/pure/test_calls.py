import logging
import typing as ty
from datetime import datetime

from thds.mops import pure

from ...config import TEST_TMP_URI
from .test_metadata import extract_memo_uris

pure.magic.blob_root(TEST_TMP_URI)
pure.magic.pipeline_id(f"test/pure-magic/{datetime.utcnow().isoformat()}")


@pure.magic()
def identity(a: ty.Union[str, int]) -> ty.Union[str, int]:
    """
    function-logic-key: v2
    """
    return a * 1


@pure.magic(calls=[identity])
def multiply(a: int, b: str) -> str:
    return int(identity(a)) * str(identity(b))


def test_calls_with_flks(caplog):
    with caplog.at_level(logging.INFO):
        assert multiply(2, "2") == "22"

    memo_uris = list(extract_memo_uris(caplog.records))
    assert len(memo_uris) == 3
    for memo_uri in memo_uris:
        assert "--identity@v2" in memo_uri
    assert "--multiply" in memo_uris[0]
