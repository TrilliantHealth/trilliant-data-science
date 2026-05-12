"""End-to-end tests for the use_runner bypass mechanism's load-bearing use cases.

The bypass exists to let the runner's remote entry point invoke a pickled function
without re-dispatching that call back through mops, while still letting *recursive*
or *cross-function* calls inside the body re-enter the runner so they get their own
memo URIs.

Cases covered:

- mutual recursion across two @pure.magic functions (samethread shim, real runner)
- recursion in a __main__ module — the docs/fibonacci.py scenario, run as a
  subprocess so it actually exercises `__main__` resolution in PicklableFunction
- recursion through subprocess_shim — every recursive call spawns its own child
  process, the same shape k8s_shim takes on a real cluster

The UA-style 'manifest' pattern (an outer @pure.magic function body iterating a
list of memo URIs and calling run_pickled_invocation on each) is not covered
here. The bypass-mechanism property the pattern depends on (a @pure.magic body
can do arbitrary work without bypassing inner wrapped calls) is exercised by
the mutual-recursion test below; the UA-specific surface around manifest
construction and lock state is too tightly coupled to K8sJobBatchingShim to
replicate cleanly here.
"""

import os
import subprocess
import sys
import tempfile
from datetime import datetime
from pathlib import Path

import pytest

from thds.mops import pure

from ...config import TEST_TMP_URI

_PIPELINE_ID_BASE = f"test/runner-reentry/{datetime.utcnow().isoformat()}"


def _local_blob_root() -> Path | None:
    if not TEST_TMP_URI.startswith("file://"):
        return None

    return Path(TEST_TMP_URI.removeprefix("file://"))


def _memo_dir(blob_root: Path, pipeline_id: str, func_module: str, func_name: str) -> Path:
    return blob_root / "mops2-mpf" / pipeline_id / f"{func_module}--{func_name}"


def _count_memo_uris(memo_dir: Path) -> int:
    if not memo_dir.exists():
        return 0

    return sum(1 for p in memo_dir.iterdir() if p.is_dir())


@pytest.fixture
def clear_magic():
    pure._magic.api._MAGIC_CONFIG = pure._magic.sauce.new_config()  # type: ignore
    yield
    pure._magic.api._MAGIC_CONFIG = pure._magic.sauce.new_config()  # type: ignore


# ---------------------------------------------------------------------------
# Mutual recursion through real MemoizingPicklingRunner + samethread shim
# ---------------------------------------------------------------------------

_MUTUAL_PIPELINE = f"{_PIPELINE_ID_BASE}/mutual"


@pure.magic(blob_root=TEST_TMP_URI, pipeline_id=_MUTUAL_PIPELINE)
def _is_even(n: int) -> bool:
    if n == 0:
        return True

    return _is_odd(n - 1)


@pure.magic(blob_root=TEST_TMP_URI, pipeline_id=_MUTUAL_PIPELINE)
def _is_odd(n: int) -> bool:
    if n == 0:
        return False

    return _is_even(n - 1)


def test_mutual_recursion_each_call_gets_its_own_memo(clear_magic):
    assert _is_even(4) is True

    root = _local_blob_root()
    if root is None:
        pytest.skip("blob_root is not local; cannot count memo URIs on disk")

    even_dir = _memo_dir(root, _MUTUAL_PIPELINE, __name__, "_is_even")
    odd_dir = _memo_dir(root, _MUTUAL_PIPELINE, __name__, "_is_odd")

    # _is_even(4) -> _is_odd(3) -> _is_even(2) -> _is_odd(1) -> _is_even(0).
    # So _is_even sees args 4, 2, 0 (3 URIs) and _is_odd sees args 3, 1 (2 URIs).
    assert _count_memo_uris(even_dir) == 3, sorted(p.name for p in even_dir.iterdir())
    assert _count_memo_uris(odd_dir) == 2, sorted(p.name for p in odd_dir.iterdir())


# ---------------------------------------------------------------------------
# A-calls-B: outer mops function's body calls a different inner mops function
# ---------------------------------------------------------------------------
#
# This is the shape the unified-asset integration tests exercise. The outer
# function is wrapped at the call site with `pure.magic.wand(...)`, not as a
# `@pure.magic` decorator on the module-level function. So when mops's remote
# entry unpickles the invocation and runs `PicklableFunction.__call__`, it
# resolves the module attribute — which is the raw function, not the wrapper —
# and calls it directly. The wrapper's `__use_runner_wrapper` never fires for
# the outer call.
#
# The inner function IS `@pure.magic`-decorated. Calling it from the outer
# function's body must reach the runner (so memoization happens), not bypass
# the runner via the unwrap that's still open for the outer call.

_ACALLSB_PIPELINE = f"{_PIPELINE_ID_BASE}/a-calls-b"


@pure.magic(blob_root=TEST_TMP_URI, pipeline_id=_ACALLSB_PIPELINE)
def _inner_doubles(n: int) -> int:
    return n * 2


def _outer_calls_inner(n: int) -> int:
    """Outer body calls _inner_doubles. Wrapped with pure.magic.wand at the call site, not as a decorator."""
    return _inner_doubles(n) + 1


def test_outer_body_calls_inner_each_gets_its_own_memo(tmp_path, clear_magic):
    outer_wrapped = pure.magic.wand(
        blob_root=TEST_TMP_URI,
        pipeline_id=_ACALLSB_PIPELINE,
    )(_outer_calls_inner)

    assert outer_wrapped(5) == 11

    root = _local_blob_root()
    if root is None:
        pytest.skip("blob_root is not local; cannot count memo URIs on disk")

    outer_dir = _memo_dir(root, _ACALLSB_PIPELINE, __name__, "_outer_calls_inner")
    inner_dir = _memo_dir(root, _ACALLSB_PIPELINE, __name__, "_inner_doubles")

    assert _count_memo_uris(outer_dir) == 1, sorted(p.name for p in outer_dir.iterdir())
    assert _count_memo_uris(inner_dir) == 1, sorted(p.name for p in inner_dir.iterdir())


# ---------------------------------------------------------------------------
# Recursion in a __main__ module (the docs/fibonacci.py scenario)
# ---------------------------------------------------------------------------


def test_main_module_recursion_each_call_gets_its_own_memo(tmp_path, clear_magic):
    # Use a hardcoded, absolute blob_root so the child subprocess sees the same
    # location as this test. The pytest --test-uri-root override only mutates
    # the parent process's TEST_TMP_URI; child processes re-import the config
    # module fresh and would otherwise see whatever defaults.mops_root() resolves
    # to in their own environment.
    blob_root_dir = tmp_path / "blob-root"
    blob_root_dir.mkdir()
    blob_root_uri = f"file://{blob_root_dir}"
    pipeline_id = "test/runner-reentry/mainmodule"

    script = tmp_path / "fib_main.py"
    script.write_text(
        # The script runs as __main__, so this exercises the PicklableFunction
        # __main__-resolution path that docs/fibonacci.py uses.
        "import sys\n"
        "from thds.mops import pure\n"
        "\n"
        f"@pure.magic(blob_root={blob_root_uri!r}, pipeline_id={pipeline_id!r})\n"
        "def fibonacci(n):\n"
        "    if n <= 1:\n"
        "        return n\n"
        "    return fibonacci(n - 1) + fibonacci(n - 2)\n"
        "\n"
        "if __name__ == '__main__':\n"
        "    print(fibonacci(int(sys.argv[1])))\n"
    )

    result = subprocess.run(
        [sys.executable, str(script), "5"],
        capture_output=True,
        text=True,
        env={**os.environ},
        check=True,
    )
    assert result.stdout.strip() == "5"

    # fib(5) recursively touches 5, 4, 3, 2, 1, 0 — six unique arg tuples.
    memo_dir = _memo_dir(blob_root_dir, pipeline_id, "__main__", "fibonacci")
    assert _count_memo_uris(memo_dir) == 6, sorted(p.name for p in memo_dir.iterdir())


# ---------------------------------------------------------------------------
# Recursion through subprocess_shim — every recursive call spawns a new process
# ---------------------------------------------------------------------------
#
# subprocess_shim is the lightest-weight non-trivial shim: each call to the
# wrapped function spawns a `python -m thds.mops.pure.core.entry.main <memo_uri>`
# subprocess that unpickles the invocation, runs it, and writes the result back.
# This is the same shape k8s_shim takes (modulo the actual k8s API call), so
# making sure recursive @pure.magic works under subprocess_shim is the closest
# we can get to "would this work on k8s" without standing up a cluster.
#
# Test-infra constraints:
#
# - The recursive function must NOT live in `__main__`. Children running
#   `python -m thds.mops.pure.core.entry.main` have their own `__main__`, and
#   `PicklableFunction._resolve()` would fail to find the function by name.
#   So we put the function in a real importable module-on-sys.path.
# - The function's @pure.magic blob_root/pipeline_id are evaluated when the
#   module is imported — both in the parent and in each child — so they must
#   resolve to the same values. We pass them via env vars rather than reading
#   the pytest-configured TEST_TMP_URI, because pytest's --test-uri-root only
#   mutates the parent process's view.
# - The driver that calls `fibonacci(N)` does have to live in some script
#   reachable as a separate process. We use a tiny one-line driver in tmp_path.

_SUBPROC_FIB_HELPER = """\
import os
from thds.mops import pure


@pure.magic(
    "subprocess",
    blob_root=os.environ["MOPS_REENTRY_TEST_BLOB_ROOT"],
    pipeline_id=os.environ["MOPS_REENTRY_TEST_PIPELINE_ID"],
)
def fibonacci(n):
    if n <= 1:
        return n
    return fibonacci(n - 1) + fibonacci(n - 2)
"""

_SUBPROC_FIB_DRIVER = """\
import sys
import subproc_fib_helper

if __name__ == "__main__":
    print(subproc_fib_helper.fibonacci(int(sys.argv[1])))
"""


def test_subprocess_shim_recursion_each_call_gets_its_own_memo():
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        blob_root_dir = tmp_path / "blob-root"
        blob_root_dir.mkdir()
        helper_dir = tmp_path / "helpers"
        helper_dir.mkdir()
        (helper_dir / "subproc_fib_helper.py").write_text(_SUBPROC_FIB_HELPER)
        driver_script = tmp_path / "drive.py"
        driver_script.write_text(_SUBPROC_FIB_DRIVER)

        env = {
            **os.environ,
            "MOPS_REENTRY_TEST_BLOB_ROOT": f"file://{blob_root_dir}",
            "MOPS_REENTRY_TEST_PIPELINE_ID": "test/runner-reentry/subprocess",
            "PYTHONPATH": f"{helper_dir}{os.pathsep}{os.environ.get('PYTHONPATH', '')}",
        }
        result = subprocess.run(
            [sys.executable, str(driver_script), "3"],
            capture_output=True,
            text=True,
            env=env,
            check=True,
        )
        assert result.stdout.strip() == "2"

        memo_dir = _memo_dir(
            blob_root_dir,
            "test/runner-reentry/subprocess",
            "subproc_fib_helper",
            "fibonacci",
        )
        # fib(3) reaches 3, 2, 1, 0 — four unique arg tuples.
        assert _count_memo_uris(memo_dir) == 4, (
            f"got {sorted(p.name for p in memo_dir.iterdir())}\n"
            f"stdout: {result.stdout}\nstderr: {result.stderr}"
        )
