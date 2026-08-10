import pytest

from thds.mops.pure.adls import listing


@pytest.mark.parametrize("directory_marker", ["true", True], ids=["string", "boolean"])
def test_directory_entries_are_skipped_however_the_service_spells_them(directory_marker):
    """The service has sent `isDirectory` as the string "true" and could as easily send a
    boolean - an equality test against either spelling silently accepts the other's
    directories as files, and a directory yielded as an event object fails to download."""
    payload = {
        "paths": [
            {"name": "events", "isDirectory": directory_marker},
            {"name": "events/e1.json", "lastModified": "Wed, 12 Aug 2026 12:00:00 GMT"},
        ]
    }

    assert [entry.name for entry in listing._files_in(payload)] == ["events/e1.json"]


def test_entries_without_the_marker_are_files():
    assert [e.name for e in listing._files_in({"paths": [{"name": "f.json"}]})] == ["f.json"]


def test_a_missing_last_modified_is_none_rather_than_an_error():
    (entry,) = listing._files_in({"paths": [{"name": "f.json"}]})

    assert entry.last_modified is None
