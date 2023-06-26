import os
import random
from pathlib import Path
from unittest import mock

import pytest

from thds.adls import resource
from thds.adls.fqn import AdlsFqn
from thds.adls.resource import AdlsHashedResource, get_read_only, upload, verify_or_create
from thds.adls.resource.file_pointers import MustCommitResourceLocally
from thds.adls.ro_cache import global_cache

DATA_DIR = Path(__file__).parent.parent / "data"
HW = DATA_DIR / "hello_world.txt"


def test_serde():
    serialized = '{"uri": "adls://foo/bar/baz", "md5b64": "WPMVPiXYwhMrMjF87w3GvA=="}'
    assert AdlsHashedResource.parse(serialized).serialized == serialized


def test_write_through_cache_upload():
    dest = AdlsFqn.of("thdsscratch", "tmp", "test/hello_world_cached.txt")
    upload(
        dest,
        HW,
        write_through_cache=global_cache(),
    )
    dpath = global_cache().path(dest)
    assert dpath.exists()


def test_write_through_cache_upload_bytes():
    dest = AdlsFqn.of("thdsscratch", "tmp", "test/hello_world_cached_bytes.txt")
    upload(
        dest,
        b"1234134132413132413412341341241234124312412341234",
        write_through_cache=global_cache(),
    )
    upload(  # it works a second time despite read-only entry
        dest,
        b"1234134132413132413412341341241234124312412341234",
        write_through_cache=global_cache(),
    )
    dpath = global_cache().path(dest)
    assert dpath.exists()
    dpath.unlink()


def test_write_through_cache_upload_readable():
    dest = AdlsFqn.of("thdsscratch", "tmp", "test/hello_world_cached_readable.txt")
    with open(HW, "rb") as f:
        upload(dest, f, write_through_cache=global_cache())

    dpath = global_cache().path(dest)
    assert dpath.exists()
    dpath.unlink()


def test_upload_iterable_bytes():
    dest = AdlsFqn.of("thdsscratch", "tmp", "test/hello_world_cached_iterable.txt")
    upload(dest, (b"87987987987987897987987987987987987", b"45345435453"))
    upload(
        dest,
        (b"87987987987987897987987987987987987", b"45345435453"),
        write_through_cache=global_cache(),
    )


@mock.patch.dict(os.environ, {"CI": ""})
def test_get_or_create_resource():
    the_path = DATA_DIR / "delete_me.db"
    the_path.unlink(missing_ok=True)
    adls_pointer = DATA_DIR / "does-not-exist.adls"
    adls_pointer.unlink(missing_ok=True)

    def describe():
        return AdlsFqn.parse("adls://thdsscratch/tmp/test/delete_me.db")

    creations = 0

    def create():
        nonlocal creations
        creations += 1
        with open(the_path, "w") as f:
            f.write(str(random.random()) * 10_000)
        return the_path

    verify_or_create(adls_pointer, describe, create)
    res = verify_or_create(adls_pointer, describe, create)
    # second time does no creation.
    assert creations == 1

    assert the_path.exists()
    get_read_only(res, the_path)
    assert the_path.exists()
    the_path.unlink()
    adls_pointer.unlink()


@mock.patch.dict(os.environ, {"CI": "1"})
def test_in_ci_resource_to_path_fails():
    with pytest.raises(MustCommitResourceLocally):
        resource.to_path(
            "whatever.txt",
            AdlsHashedResource.parse(
                '{"uri": "adls://foo/bar/baz", "md5b64": "WPMVPiXYwhMrMjF87w3GvA=="}'
            ),
            check_ci=True,
        )
