"""
todo: mock the functions azure storage datalake provides so that we can test the functions locally

In the meantime, here is an example to test the module against a real azure ADLS

```
from core.adls import ADLSDownloader

downloader = ADLSDownloader(account_name="some_account_name", file_system="some_file_system")
local_path = downloader.fetch_file("remote_file_path")
local_dir_file_paths = downloader.fetch_directory("remote_dir_path")
local_table_file_paths = downloader.fetch_hive_table("database.tablename")
```

"""
import datetime
import os
import shutil
import tempfile
from pathlib import Path
from typing import NamedTuple

import pytest

from thds.adls import ADLSFileSystemCache, AdlsFqn, format_fqn, parse_fqn

TEST_PATHS = ["foo", "/bar", "foo/bar", "foo/", "bar/", "foo/bar/", "foo/bar/baz.txt"]
TEST_FILE_PATHS = [p for p in TEST_PATHS if not p.endswith("/")]
TEST_DIR_PATHS = [p for p in TEST_PATHS if p.endswith("/")]

timezones = [
    datetime.timezone.utc,
    datetime.timezone(datetime.timedelta(hours=-12)),
    datetime.timezone(datetime.timedelta(hours=-12)),
]


class MockFileProperties(NamedTuple):
    name: str
    size: int
    last_modified: datetime.datetime


@pytest.fixture(scope="module")
def cache_dir():
    cache_dir = tempfile.TemporaryDirectory()
    yield Path(cache_dir.name).absolute()
    cache_dir.cleanup()


@pytest.fixture(scope="module")
def cache(cache_dir: Path):
    cache = ADLSFileSystemCache(cache_dir)
    yield cache
    cache.clear()


@pytest.fixture(scope="function", params=TEST_FILE_PATHS)
def cache_file_path(cache: ADLSFileSystemCache, request):
    path = request.param
    cache_path = cache.cache_path(path)
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    if cache_path.is_dir():
        shutil.rmtree(cache_path)
    cache_path.touch()

    yield path

    if cache_path.exists():
        os.remove(cache_path)
        while cache_path.parent != cache.cache_dir:
            cache_path = cache_path.parent
        if cache_path.exists():
            shutil.rmtree(cache_path)


@pytest.fixture(scope="function", params=timezones)
def cache_file_properties_future(cache: ADLSFileSystemCache, cache_file_path: str, request):
    return _cache_file_properties(cache, cache_file_path, datetime.timedelta(seconds=1), request.param)


@pytest.fixture(scope="function", params=timezones)
def cache_file_properties_past(cache: ADLSFileSystemCache, cache_file_path: str, request):
    return _cache_file_properties(cache, cache_file_path, datetime.timedelta(seconds=-1), request.param)


def _cache_file_properties(cache, cache_file_path, offset, tz):
    stat = os.stat(cache.cache_path(cache_file_path))
    return MockFileProperties(
        name=cache_file_path,
        size=stat.st_size,
        last_modified=datetime.datetime.fromtimestamp(stat.st_mtime, tz=tz) + offset,
    )


@pytest.fixture(scope="function", params=TEST_DIR_PATHS)
def cache_dir_path(cache: ADLSFileSystemCache, request):
    path = request.param
    cache_path = cache.cache_path(path)
    cache_path.mkdir(parents=True, exist_ok=True)
    yield path
    if cache_path.exists():
        shutil.rmtree(cache_path)


@pytest.mark.parametrize("path", TEST_PATHS)
def test_cache_path(cache: ADLSFileSystemCache, cache_dir: Path, path: str):
    cache_path = cache.cache_path(path)
    suffix = cache_path.relative_to(cache_dir)
    suffix_parts = suffix.parts
    parts = path.strip("/").split("/")
    assert list(parts) == list(suffix_parts)


def test_cache_contains_file(cache: ADLSFileSystemCache, cache_file_path: str):
    assert cache_file_path in cache


def test_cache_not_contains_dir(cache: ADLSFileSystemCache, cache_dir_path: str):
    assert cache_dir_path not in cache


def test_cache_remove_file(cache: ADLSFileSystemCache, cache_file_path: str):
    cache.remove(cache_file_path)
    assert cache_file_path not in cache


def test_cache_remove_dir(cache: ADLSFileSystemCache, cache_dir_path: str):
    cache.remove(cache_dir_path)
    assert cache_dir_path not in cache
    assert not cache.cache_path(cache_dir_path).exists()


def test_cache_not_valid(
    cache: ADLSFileSystemCache,
    cache_file_properties_future: MockFileProperties,
):
    assert not cache.is_valid_for(cache_file_properties_future)  # type: ignore


def test_cache_valid(
    cache: ADLSFileSystemCache,
    cache_file_properties_past: MockFileProperties,
):
    assert cache.is_valid_for(cache_file_properties_past)  # type: ignore


def test_adls_fqn():
    old_name = "thdsdatasets prod-datasets /lib-datamodel/v22/backerd compat.jsonl"
    name = "adls://thdsdatasets/prod-datasets/lib-datamodel/v22/backerd compat.jsonl"
    fqn = parse_fqn(old_name)

    assert fqn.sa == "thdsdatasets"
    assert fqn.container == "prod-datasets"
    assert fqn.path == "lib-datamodel/v22/backerd compat.jsonl"

    assert format_fqn(*fqn) == name

    assert format_fqn(fqn.sa, fqn.container, fqn.path) == name
    # we add the forward slash in front of the path.

    assert str(fqn) == name

    assert AdlsFqn.parse(name) == parse_fqn(name)
    assert AdlsFqn.parse(old_name) == parse_fqn(name)

    joined = parse_fqn("sa1 cont /path/to/dir/").join("/somedir").path
    assert "path/to/dir/somedir" in joined, joined  # no double slash
    assert "path/foo/bar/baz" in parse_fqn("sa2 cont2 path/foo/bar").join("baz").path
