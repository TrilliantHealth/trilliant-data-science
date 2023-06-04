from pathlib import Path

from thds.mops.remote import DestFile, SrcFile, adls_dataset_context
from thds.mops.remote.core import invocation_unique_key

from ._util import adls_shell


@adls_shell
def mul(a: int, b: float = 4.2) -> float:
    in_un_key = invocation_unique_key()
    assert in_un_key
    assert in_un_key.endswith("c/c470c619e86f77fdda807f68bd101ef58c0d1a42e9c464e08578e8d7a5dda0b")
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
    memo_key = invocation_unique_key()
    assert memo_key
    assert memo_key.endswith("0/dd6c8dcde4ebf49bcfa6948bce3c854039f94cbeb687f57ad40725af5030ad3")
    print("Confirmed that serialization remains stable for SrcFile, DestFile, and Path")
    with sfile as s1:
        with open(s1) as s:
            with open(path) as p:
                txt = s.read() + p.read()
                with dfile as d:
                    with open(d, "w") as out:
                        out.write(txt)
    return dfile


_stable_context = adls_dataset_context(
    "test-adls-src-dest-files/",
    "tests/test_mops/integration",  # ignore this part of the local file path
)


def test_hello_world_srcdestpath_memoization_is_stable():
    sfile = _stable_context.src("tests/test_mops/integration/remote/hello.txt")
    path = Path("tests/test_mops/integration/remote/world.txt")
    dfile = _stable_context.dest("tests/test_mops/integration/remote/hello_world.txt")

    # must convert the returned DestFile into a SrcFile so that we can read its contents.
    with _stable_context.src(hello_world_src_dest_path(dfile, sfile, path)) as tmp:
        with open(tmp) as f:
            assert f.read() == "hello\nworld\n"
