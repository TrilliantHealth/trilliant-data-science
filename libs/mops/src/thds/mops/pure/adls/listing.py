"""Directory listing against the Data Lake REST API.

Exists because `azure-storage-file-datalake` 12.18.1 does not plumb the REST API's
`beginFrom` parameter to any of its layers - not `get_paths`, not `PathPropertiesPaged`, not
the generated `FileSystemOperations.list_paths`. Resuming a listing partway through is the
one thing that makes reading a large prefix incrementally affordable, so it is worth reaching
past the SDK's surface for.

Only the request construction is hand-rolled; auth, retries and transport remain the SDK's
own pipeline. Delete this in favour of `get_paths(start_from=)` once the pinned SDK
supports it.
"""

import datetime as dt
import json
import typing as ty
from email.utils import parsedate_to_datetime
from urllib.parse import quote

from azure.core.exceptions import HttpResponseError
from azure.core.pipeline.transport import HttpRequest
from azure.storage.filedatalake import FileSystemClient

from thds import adls


class Listed(ty.NamedTuple):
    name: str
    last_modified: None | dt.datetime


def _parse_rfc1123(value: str) -> None | dt.datetime:
    if not value:
        return None

    return parsedate_to_datetime(value)


def _query(directory: str, begin_from: str, continuation: str) -> str:
    return "&".join(
        f"{key}={value}"
        for key, value in (
            ("resource", "filesystem"),
            ("directory", quote(directory)),
            ("recursive", "false"),
            ("beginFrom", quote(begin_from)),
            *((("continuation", quote(continuation)),) if continuation else ()),
        )
    )


def _request(fqn: adls.AdlsFqn, api_version: str, query: str) -> HttpRequest:
    request = HttpRequest("GET", f"https://{fqn.sa}.dfs.core.windows.net/{fqn.container}?{query}")
    request.headers["x-ms-version"] = api_version
    request.headers["Accept"] = "application/json"
    # the SDK's generated builders set both per-request, and this bypasses them. Without an
    # explicit version the service picks its own default, and one predating `beginFrom`
    # would ignore the parameter and return the whole directory rather than failing.
    return request


def _files_in(payload: ty.Mapping[str, ty.Any]) -> ty.Iterator[Listed]:
    """The file entries of one listing response.

    Directories are skipped by the truthiness of `isDirectory`, which the service has
    sent as the string "true" and could as easily send as a boolean - an equality test
    against either spelling silently accepts the other's directories as files.
    """
    for path in payload.get("paths", []):
        if not path.get("isDirectory"):
            yield Listed(path["name"], _parse_rfc1123(path.get("lastModified", "")))


def paths_from(fs_client: FileSystemClient, fqn: adls.AdlsFqn, begin_from: str) -> ty.Iterator[Listed]:
    """Entries directly under `fqn`, starting at the entry named `begin_from`.

    `begin_from` is relative to the listed directory and inclusive of the entry it names
    (measured against the service; Microsoft documents the parameter but not its
    inclusivity). It need not exist - it is a position in the sort order, not a lookup.
    """
    continuation = ""
    while True:
        response = fs_client._client._client._pipeline.run(
            _request(fqn, fs_client._client._config.version, _query(fqn.path, begin_from, continuation))
        ).http_response
        if response.status_code >= 400:
            raise HttpResponseError(response=response)

        yield from _files_in(json.loads(response.text()))

        continuation = response.headers.get("x-ms-continuation") or ""
        if not continuation:
            return
