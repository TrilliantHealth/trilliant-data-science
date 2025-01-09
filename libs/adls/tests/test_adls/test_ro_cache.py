import random

from thds.adls.ro_cache import AdlsFqn, Cache, Path, _cache_path_for_fqn


def test_long_path_parts_are_compressed(tmp_path: Path):
    test_cache = Cache(tmp_path, ("ref",))

    cache_path = _cache_path_for_fqn(
        test_cache, AdlsFqn("sa", "cont", f"foobar/{'a' * 256}/{256 * 'b'}/cccc")
    )

    assert str(cache_path) == (
        str(tmp_path / "sa/cont/foobar/")
        + f"/{'a' * 109}-md5-81109eec5aa1a284fb5327b10e9c16b9-{'a' * 108}"
        + f"/{'b' * 109}-md5-91bfe9a0d83cfafac19af2adcffeec7a-{'b' * 108}"
        + "/cccc"
    )


def _all_unicode_path_chars() -> str:
    return "".join(
        tuple(chr(i) for i in range(32, 0x110000) if chr(i).isprintable() and not chr(i) == "/")
    )


UNICODE = _all_unicode_path_chars()


def _create_long_filename(num_parts: int) -> str:
    def random_long_unicode_str_from_stackoverflow() -> str:
        # https://stackoverflow.com/a/39682429
        return "".join(random.sample(UNICODE, random.randint(256, 300)))

    return "/".join([random_long_unicode_str_from_stackoverflow() for _ in range(num_parts)])


def test_random_large_path_parts_can_be_created_on_the_filesystem(tmp_path: Path):
    test_cache = Cache(tmp_path, ("ref",))

    num_parts = 3
    # more than 3 parts runs into issues with the filename being too large overall.
    # the behavior under those circumstances is a separate test below this one.
    path = _cache_path_for_fqn(
        test_cache,
        AdlsFqn(
            "sa",
            "cont",
            _create_long_filename(num_parts) + ".parquet",
        ),
    )

    assert (
        str(path).count("-md5-") >= num_parts
    )  # could theoretically occur naturally in the original str

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("wrote file successfully")
    # if the above succeeds, then we've successfully made the path short enough for use...
    assert path.read_text() == "wrote file successfully"


def test_very_long_path(tmp_path: Path):
    """The filesystems also have limits on how long your total path can be. This tests 10
    parts of 255 characters each, which will easily exceed those limits on both Mac and
    Linux."""
    test_cache = Cache(tmp_path, ("ref",))

    path = _cache_path_for_fqn(
        test_cache,
        AdlsFqn(
            "sa",
            "cont",
            _create_long_filename(20) + ".parquet",
        ),
    )

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("wrote file successfully")
    # if the above succeeds, then we've successfully made the path short enough for use...
    assert path.read_text() == "wrote file successfully"
