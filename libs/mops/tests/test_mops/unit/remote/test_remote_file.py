from thds.mops.remote._destfile import DestFile, trigger_dest_files_placeholder_write


def test_trigger_dest_files_placeholder_write(caplog):
    df = DestFile(trigger_dest_files_placeholder_write, "foobar")  # type: ignore
    trigger_dest_files_placeholder_write([1, 2, tuple(), dict(a=df)])

    assert "DestFile" in caplog.records[0].msg
    assert "empty serialization" in caplog.records[0].msg
