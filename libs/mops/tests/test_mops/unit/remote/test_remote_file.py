import tempfile

from thds.mops.remote.remote_file import DestFile, trigger_dest_files_download


def test_trigger_dest_files_download(caplog):

    with tempfile.NamedTemporaryFile() as f:
        df = DestFile(f.name, "foobar")  # type: ignore
        trigger_dest_files_download([1, 2, tuple(), dict(a=df)])

        assert "DestFile" in caplog.records[0].msg
        assert "empty serialization" in caplog.records[0].msg
