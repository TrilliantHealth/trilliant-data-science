import pickle

from thds.mops.pure.core.memo.results import RequiredResultNotFound


def test_required_result_not_found_survives_pickling():
    """A process pool hands exceptions back pickled; the default reconstruction would call
    __init__ with the message alone and fail with a TypeError instead of the real error."""
    exc = RequiredResultNotFound("Required a result for X but that result was not found", "adls://a/b")
    back = pickle.loads(pickle.dumps(exc))
    assert isinstance(back, RequiredResultNotFound)
    assert str(back) == str(exc)
    assert back.uri == "adls://a/b"
