from thds.mops import pure, tempdir
from thds.mops.srcdest import DestFile


@pure.memoize_in("adls://thdsscratch/tmp")
def remote_func(i: int) -> DestFile:
    outfile = tempdir() / "foo.txt"
    with open(outfile, "w") as wf:
        wf.write(str(i))
    return pure.adls.rdest(outfile)


def test_rdest():
    df = pure.adls.src_from_dest(remote_func(1))
    with df as lcl:
        assert lcl.exists()
        assert open(lcl).read() == "1"
