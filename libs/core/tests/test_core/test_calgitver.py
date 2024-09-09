from thds.core.meta import make_calgitver, parse_calgitver


def test_calgitver():
    """This is not an exhaustive test by any means. It's just a sanity check for some fairly simple code."""
    cgv = make_calgitver()
    print(cgv)
    chd = cgv.split("-")
    if len(chd) == 3:
        cal, git_hash, git_is_dirty = chd
        assert git_is_dirty == "dirty"
    else:
        cal, git_hash = chd
    assert len(git_hash) == 7  # std short hash length
    assert len(cal) == 13
    yyyymmdd, hhmm = cal.split(".")
    assert len(yyyymmdd) == 8
    assert len(hhmm) == 4
    yyyymmdd.startswith("20")  # this is gonna break someday :(
    assert hhmm[0] in ("0", "1", "2")


def test_parse_calgitver():
    gd = parse_calgitver("20021130.0804-34afb29-dirty").groupdict()
    assert gd == dict(
        year="2002",
        month="11",
        day="30",
        hour="08",
        minute="04",
        git_commit="34afb29",
        dirty="-dirty",
    )

    n = parse_calgitver("20021130.0804-34afb29")
    assert n.group("dirty") == ""
