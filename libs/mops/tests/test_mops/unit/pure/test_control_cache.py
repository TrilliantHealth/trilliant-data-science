from pathlib import Path
from uuid import uuid4

from thds.core import tmp
from thds.mops.pure.core.control_cache import exists_with_expiry


def test_exists_with_expiry_when_does_not_exist_returns_false():
    path = Path(f"/some/path/{uuid4().hex}")
    assert exists_with_expiry(path, 3600) is False
    assert path.exists() is False


def test_exists_with_expiry_when_exists_and_not_expired_returns_true():
    with tmp.temppath_same_fs() as tp:
        with open(tp, "w") as f:
            f.write("foo")
        assert exists_with_expiry(tp, 3600) is True
        assert tp.exists() is True


def test_exists_with_expiry_when_exists_and_expired_unlinks_and_returns_false():
    with tmp.temppath_same_fs() as tp:
        with open(tp, "w") as f:
            f.write("foo")
        assert exists_with_expiry(tp, 0) is False
        assert tp.exists() is False
