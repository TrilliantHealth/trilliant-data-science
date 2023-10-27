from pathlib import Path

from thds.adls import AdlsRoot
from thds.mops.pure import adls
from thds.mops.pure.core.output_naming import pipeline_function_invocation_unique_key
from thds.mops.srcdest import DestFile, SrcFile

from ...config import TEST_TMP_URI
from ._util import adls_shell


@adls_shell
def mul(a: int, b: float = 4.2) -> float:
    in_un_key = pipeline_function_invocation_unique_key()
    assert in_un_key
    pf, fa = in_un_key
    assert fa == "SplitJarBlame.YZ6G93_dqAf2i9EB71jA0aQunEZOCFeOjXpd2gs", in_un_key
    return a * b


def test_memoization_of_args_kwargs_is_stable_across_different_looking_call_signatures():
    assert 16.8 == mul(4, 4.2)
    assert 16.8 == mul(4, b=4.2)
    assert 16.8 == mul(a=4, b=4.2)
    assert 16.8 == mul(b=4.2, a=4)
    assert 16.8 == mul(a=4)


@adls_shell
def hello_world_src_dest_path(dfile: DestFile, sfile: SrcFile, path: Path) -> DestFile:
    """This exercises all our file and file-like abstractions to make
    sure their memoization remains stable across time.
    """
    memo_key = pipeline_function_invocation_unique_key()
    assert memo_key
    pf, fa = memo_key
    assert fa == "ClimbFewPushy.-FCkuTcpoYtmnNUm8YXnjnDV9ply3VxjHi5v4_0", memo_key
    print("Confirmed that serialization remains stable for SrcFile, DestFile, and Path")
    # this depends on lots of things not changing, including the URIs
    # we use for content addressing.
    with sfile as s1:
        with open(s1) as s:
            with open(path) as p:
                txt = s.read() + p.read()
                with dfile as d:
                    with open(d, "w") as out:
                        out.write(txt)
    return dfile


test_root = AdlsRoot.parse(TEST_TMP_URI) / "test-adls-src-dest-files"


def test_hello_world_srcdestpath_memoization_is_stable():
    sfile = adls.local_src(
        test_root / "tests/test_mops/integration/pure/hello.txt",
        "tests/test_mops/integration/pure/hello.txt",
    )
    path = Path("tests/test_mops/integration/pure/world.txt")
    dfile = adls.dest(test_root / "tests/test_mops/integration/pure/hello_world.txt", "")

    # must convert the returned DestFile into a SrcFile so that we can read its contents.
    with adls.src_from_dest(hello_world_src_dest_path(dfile, sfile, path)) as tmp:
        with open(tmp) as f:
            assert f.read() == "hello\nworld\n"
