from thds.core.meta import make_calgitver


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
