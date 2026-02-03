import json
import shutil
from datetime import datetime
from pathlib import Path
from typing import Generator
from unittest.mock import MagicMock

import pytest

from thds.mops.pure import MemoizingPicklingRunner
from thds.mops.pure.tools.summarize import run_summary


@pytest.fixture(scope="session", autouse=True)
def set_env() -> None:
    # Fixture to set the root log directory name
    run_summary.MOPS_SUMMARY_DIR.set_global(Path(".mops-test"))


@pytest.fixture
def run_directory() -> Generator[Path, None, None]:
    # Fixture for creating and supplying a run directory to a test
    # and clean it up afterward
    run_dir = run_summary.create_mops_run_directory()

    yield run_dir

    shutil.rmtree(run_dir)


def test_create_mops_directory(run_directory: Path) -> None:
    assert run_directory.exists()
    assert run_directory.is_dir()


def test_memoizing_pickling_runner_init() -> None:
    mock_shim = MagicMock()
    storage_root = "file:///mock_storage_root"
    runner = MemoizingPicklingRunner(mock_shim, blob_storage_root=storage_root)
    assert runner._run_directory.exists()
    assert runner._run_directory.is_dir()

    shutil.rmtree(runner._run_directory)


def test_log_function_execution_new_file(run_directory: Path) -> None:
    memo_uri = "adls://env/foo/bar/pipeline-id/complex/the.module--function_id_new/ARGS"

    run_summary.log_function_execution(
        run_directory,
        memo_uri,
        itype="invoked",
        runner_prefix="adls://env/foo/bar/",
    )

    log_files = list(run_directory.glob("*.json"))
    assert len(log_files) == 1

    log_file = log_files[0]
    with log_file.open() as f:
        log_data: run_summary.LogEntry = json.load(f)

    assert log_data["function_name"] == "the.module:function_id_new"
    assert log_data["memo_uri"] == memo_uri
    assert log_data["status"] == "invoked"
    assert datetime.fromisoformat(log_data["timestamp"])
    assert log_data["runner_prefix"] == "adls://env/foo/bar"


def test_log_function_execution_invalid_json(run_directory: Path) -> None:
    memo_uri = "adls://env/mops2-mpf/pipeline-id/some-path/foo.bar--function-id-invalid-json-test/ARGS"

    log_file = run_directory / "invalid.json"

    # Create an invalid JSON file
    with log_file.open("w") as f:
        f.write("invalid json")

    # Log a new execution
    run_summary.log_function_execution(run_directory, memo_uri, itype="invoked")

    new_log_files = list(run_directory.glob("*.json"))
    assert len(new_log_files) == 2

    # Find the new valid log file
    new_log_file = next(file for file in new_log_files if file != log_file)
    with new_log_file.open() as f:
        log_data: run_summary.LogEntry = json.load(f)

    assert log_data["function_name"] == "foo.bar:function-id-invalid-json-test"
    assert log_data["memo_uri"] == memo_uri
    assert log_data["status"] == "invoked"
    assert datetime.fromisoformat(log_data["timestamp"])
