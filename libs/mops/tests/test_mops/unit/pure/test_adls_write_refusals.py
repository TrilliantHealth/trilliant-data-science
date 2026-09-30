import pytest
from azure.core.exceptions import HttpResponseError

from thds import adls
from thds.mops.pure.adls import blob_store


def _http_error(code: str, status: int = 403) -> HttpResponseError:
    err = HttpResponseError("the service said no")
    err.error_code = code  # type: ignore[attr-defined]  # set by the storage SDK, not azure-core
    err.status_code = status
    return err


def test_a_write_the_role_does_not_allow_is_a_permission_error_and_is_not_retried(monkeypatch):
    attempts: list[str] = []

    def upload(uri, data, content_type):
        attempts.append(uri)
        raise _http_error("AuthorizationPermissionMismatch")

    monkeypatch.setattr(adls, "upload", upload)

    with pytest.raises(PermissionError):
        blob_store.AdlsBlobStore().putbytes("adls://sa/container/a/b", b"x")

    assert len(attempts) == 1


def test_other_http_errors_are_not_translated(monkeypatch):
    def upload(uri, data, content_type):
        raise _http_error("PathNotFound", status=404)
        # a 404 is not retried either, so the test does not sleep through the backoff.

    monkeypatch.setattr(adls, "upload", upload)

    with pytest.raises(HttpResponseError):
        blob_store.AdlsBlobStore().putbytes("adls://sa/container/a/b", b"x")
