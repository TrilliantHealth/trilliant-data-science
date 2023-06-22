from pathlib import Path

from thds.adls.fqn import AdlsFqn
from thds.adls.resource import AdlsHashedResource, upload
from thds.adls.ro_cache import global_cache

HW = Path(__file__).parent.parent / "data/hello_world.txt"


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
