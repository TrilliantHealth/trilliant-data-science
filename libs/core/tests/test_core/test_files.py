from thds.core.files import shorten_filename


def test_shorten_filename():
    assert shorten_filename("a" * 200) == "a" * 200
    # no change, because this is not too long.

    shortened = shorten_filename("a" * 300)
    assert shortened.startswith("aaaaa")
    assert shortened.endswith("-" + "a" * 30)
    assert "-md5-" in shortened
    assert len(shortened.encode()) == 255
