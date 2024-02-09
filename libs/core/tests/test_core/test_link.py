import concurrent.futures
import platform
import random

from thds.core import link


def test_reflink_works_on_mac(tmp_path, temp_file):
    if platform.system() != "Darwin":
        return

    fb = temp_file("foobar")

    dest = tmp_path / "foobar-reflink"
    assert "ref" == link.link(fb, dest)

    assert dest.read_text() == fb.read_text()


def test_samelink_works(tmp_path):
    fb = tmp_path / "foobar"
    fb.write_text("fooooooo")
    fb2 = fb.parent / "foobar"

    assert "same" == link.link(fb, fb2)

    assert fb2.read_text() == fb.read_text()


def test_hardlink_works(tmp_path):
    hlsrc = tmp_path / "hardlink"
    hlsrc.write_text("hardlink-data")
    dest = tmp_path / "foobar-hardlink"

    assert "hard" == link.link(hlsrc, dest, "hard")

    assert dest.read_text() == hlsrc.read_text()


def test_softlink_works(tmp_path):
    slsrc = tmp_path / "softlink"
    slsrc.write_text("softlink-data")
    dest = tmp_path / "foobar-softlink"

    assert "soft" == link.link(slsrc, dest, "soft")

    assert dest.read_text() == slsrc.read_text()


def test_links_are_atomic_even_if_dest_exists(tmp_path):
    # in order to 'test' this, we're going to have to induce
    # a race condition, with lots of different threads trying to link the same file
    # and then we will 'observe' that it has worked by seeing that we get no errors
    # and that the file ends up in the state we expect.

    dest_file = tmp_path / "dest-for-atomic-link"
    dest_file.write_text("dest-file-data")  # it will already exist when we start

    def write_new_file_then_link(i):
        src = tmp_path / f"{i}.txt"
        src.write_text("same-data-every-time")
        return link.link(src, dest_file, random.choice(["hard", "soft"]))

    with concurrent.futures.ThreadPoolExecutor(max_workers=12) as executor:
        futures = [executor.submit(write_new_file_then_link, i) for i in range(100)]
        for fut in futures:
            assert fut.result() in ("hard", "soft")

    assert dest_file.read_text() == "same-data-every-time"
