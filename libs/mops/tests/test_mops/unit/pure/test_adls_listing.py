import datetime as dt
from types import SimpleNamespace

import pytest
from azure.core.exceptions import HttpResponseError, ResourceNotFoundError

from thds.mops.pure.adls import blob_store


def _client_whose_listing_raises(exc: Exception):  # noqa: ANN202
    def get_paths(path, recursive):
        raise exc
        yield  # pragma: no cover - makes this a generator, so the raise happens on iteration

    return SimpleNamespace(get_paths=get_paths)


def test_a_prefix_nothing_was_written_under_lists_as_empty(monkeypatch):
    not_found = ResourceNotFoundError("The specified path does not exist.")
    not_found.status_code = 404
    monkeypatch.setattr(
        blob_store, "get_global_fs_client", lambda sa, c: _client_whose_listing_raises(not_found)
    )
    assert list(blob_store.AdlsBlobStore().list("adls://thdsscratch/tmp/fresh/queue/manifest")) == []


def test_listing_timestamps_are_aware_utc(monkeypatch):
    naive = dt.datetime(2026, 9, 18, 14, 0, 0)  # what the SDK's strptime of "... GMT" yields

    def get_paths(path, recursive):
        yield SimpleNamespace(name="tmp/q/manifest/000001", last_modified=naive)

    monkeypatch.setattr(
        blob_store, "get_global_fs_client", lambda sa, c: SimpleNamespace(get_paths=get_paths)
    )
    (entry,) = blob_store.AdlsBlobStore().list("adls://thdsscratch/tmp/q/manifest")
    assert entry.modified_at == naive.replace(tzinfo=dt.timezone.utc)
    assert entry.modified_at < dt.datetime.now(dt.timezone.utc)  # comparable, which is the point


def test_other_listing_errors_still_raise(monkeypatch):
    forbidden = HttpResponseError("forbidden")
    forbidden.status_code = 403
    monkeypatch.setattr(
        blob_store, "get_global_fs_client", lambda sa, c: _client_whose_listing_raises(forbidden)
    )
    with pytest.raises(HttpResponseError):
        list(blob_store.AdlsBlobStore().list("adls://thdsscratch/tmp/prefix"))
