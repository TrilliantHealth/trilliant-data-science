import io
import threading

import pytest

from thds.mops.pure import _magic, magic
from thds.mops.pure.core.types import UnpicklableRemoteException


class _ErrorHoldingALock(Exception):
    def __init__(self, msg: str) -> None:
        super().__init__(msg)
        self.lock = threading.RLock()


class _ErrorHoldingAClosedBuffer(Exception):
    def __init__(self, msg: str) -> None:
        super().__init__(msg)
        self.buffer = io.BytesIO()
        self.buffer.close()


def _fails_holding_a_lock() -> None:
    raise _ErrorHoldingALock("the input path is missing")


def _fails_holding_a_closed_buffer() -> None:
    raise _ErrorHoldingAClosedBuffer("the input path is missing")


@pytest.fixture
def clear_magic():
    _magic.api._MAGIC_CONFIG = _magic.sauce.new_config()  # type: ignore
    yield


@pytest.mark.parametrize(
    "func, exc_name",
    [
        (_fails_holding_a_lock, "_ErrorHoldingALock"),  # TypeError from pickle
        (_fails_holding_a_closed_buffer, "_ErrorHoldingAClosedBuffer"),  # ValueError from pickle
    ],
)
def test_unpicklable_exception_reaches_caller_as_text(clear_magic, tmp_path, func, exc_name):
    with pytest.raises(UnpicklableRemoteException) as exc_info:
        magic(blob_root=f"file://{tmp_path}")(func)()

    assert f"{exc_name}: the input path is missing" in str(exc_info.value)
