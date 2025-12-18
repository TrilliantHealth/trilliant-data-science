from pathlib import Path

from thds.core import source

HW = Path(__file__).parent.parent / "data/hello_world.txt"


def test_registered_upload(tmp_remote_root, random_test_file_path):
    the_uri = str(tmp_remote_root / random_test_file_path)
    src = source.from_file(HW, uri=the_uri)

    # if no exception is raised, and the URI is correct, the test passes
    assert source.upload(src).uri == the_uri
