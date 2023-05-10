import os
import tempfile
from pathlib import Path

import pytest

from thds.adls import ADLSFileSystem

HW = Path(__file__).parent.parent / "data/hello_world.txt"


def test_basic_upload_without_cache():
    fs = ADLSFileSystem("thdsscratch", "tmp")

    remote_path = "test/hello_world.txt"
    fs.put_file(HW, remote_path)
    with tempfile.TemporaryDirectory() as dir:
        hw = Path(dir) / "hw.txt"
        fs.fetch_file(remote_path, hw)
        with open(hw) as f:
            assert f.read() == open(HW).read()


@pytest.mark.skipif(
    "CI" in os.environ,
    reason="Not allowed to access uaapdatascience at all in CI",
)
def test_cached_large_upload():
    fs = ADLSFileSystem("uaapdatascience", "data")

    remote_path = "test/a_small_parquet_file.parquet"
    with tempfile.TemporaryDirectory() as dir:
        local_path = Path(dir) / "11mb.parquet"
        fs.fetch_file(remote_path, local_path)
        fs.put_file(local_path, remote_path)  # this should get skipped after md5 calc.
