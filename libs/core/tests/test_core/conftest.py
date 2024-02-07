import tempfile
import typing as ty
import uuid
from pathlib import Path

import pytest


@pytest.fixture(autouse=True)
def clear_caches():
    from thds.core import meta

    meta.read_metadata.cache_clear()
    meta.get_version.cache_clear()
    meta.get_base_package.cache_clear()


@pytest.fixture
def temp_file() -> ty.Iterator[ty.Callable[[str], Path]]:
    with tempfile.TemporaryDirectory() as tempdir:

        def make_temp_file(some_text: str) -> Path:
            p = Path(tempdir) / ("cfile-" + uuid.uuid4().hex)
            with open(p, "w") as f:
                f.write(some_text)
            return p

        yield make_temp_file
