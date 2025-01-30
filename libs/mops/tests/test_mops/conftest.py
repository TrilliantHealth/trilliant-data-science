import typing as ty
import uuid
from pathlib import Path

import pytest

from thds.mops import tempdir


def make_temp_file(some_text: str) -> Path:
    file = tempdir() / ("lfile-text-" + uuid.uuid4().hex)
    file.write_text(some_text)
    return file


@pytest.fixture
def temp_file() -> ty.Iterator[ty.Callable[[str], Path]]:
    yield make_temp_file
