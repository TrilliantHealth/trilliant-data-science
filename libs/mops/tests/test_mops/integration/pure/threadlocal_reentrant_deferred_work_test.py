# technically, mops doesn't prevent re-entrancy - in other words, a mops-run function
# calling another one internally.
#
# in practice, we rarely exercise the functionality
# because of how tricky is it to manage the real-world permissions when a remote runner
# wants to be able to invoke another remote runner.
#
# it seems wise to have a test or two in place that prove that we can actually do this
# without things breaking on mops's side, though.

from pathlib import Path
from uuid import uuid4

from thds.core import log, source, tmp
from thds.mops import pure, tempdir

MOPS_ROOT = tempdir() / ("reentrant-root-" + uuid4().hex)
# needs to be available at module context, but also needs to get cleaned up later, and
# also needs to be very 'safe'/random so as not to get stepped on.

logger = log.getLogger(__name__)


@pure.memoize_in(f"file://{MOPS_ROOT}")
def _concat_two_files(a: Path, b: source.Source) -> Path:
    """
    pipeline-id-mask: INNER
    """
    a_txt = a.read_text()
    b_txt = b.path().read_text()

    ab = tempdir() / "c.txt"
    # use mops tempdir so it sticks around after return but gets cleaned up when the interpreter exits.
    ab.write_text(a_txt + b_txt)
    logger.info(f"wrote out combined text to {ab}")
    return ab


@pure.memoize_in(f"file://{MOPS_ROOT}")
def _check_if_c_in_ab(a: Path, b: source.Source, c: Path) -> bool:
    """
    pipeline-id-mask: outer
    """
    two_paths = _concat_two_files(a, b)
    c_txt = c.read_text()
    return c_txt in two_paths.read_text()


def test_threadlocal_shell_can_do_deferred_work():
    with tmp.tempdir_same_fs() as work_dir:
        apath = work_dir / "a.txt"
        bpath = work_dir / "b.txt"
        cpath = work_dir / "c.txt"
        apath.write_text("aaaa foo")
        bpath.write_text("bar bbbb")
        cpath.write_text("foobar")
        assert _check_if_c_in_ab(apath, source.from_file(bpath), cpath)
