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
    mock_shell = MagicMock()
    storage_root = "file:///mock_storage_root"
    runner = MemoizingPicklingRunner(shell=mock_shell, blob_storage_root=storage_root)
    assert runner._run_directory.exists()
    assert runner._run_directory.is_dir()

    shutil.rmtree(runner._run_directory)


def test_log_function_execution_new_file(run_directory: Path) -> None:

    memo_uri = "adls://pipeline-id/complex/function-id-new"

    def test_function() -> None:
        pass

    run_summary.log_function_execution(run_directory, test_function, memo_uri, status="invoked")

    log_files = list(run_directory.glob("*.json"))
    assert len(log_files) == 1

    log_file = log_files[0]
    with log_file.open() as f:
        log_data: run_summary.LogEntry = json.load(f)

    assert log_data["function_name"] == "tests.test_mops.unit.pure.test_run_summary:test_function"
    assert log_data["memo_uri"] == memo_uri
    assert log_data["status"] == "invoked"
    assert datetime.fromisoformat(log_data["timestamp"])


def test_log_function_execution_invalid_json(run_directory: Path) -> None:
    memo_uri = "adls://pipeline-id/some-path/function-id-invalid-json-test"

    def test_function() -> None:
        pass

    log_file = run_summary._generate_log_filename(run_directory)

    # Create an invalid JSON file
    with log_file.open("w") as f:
        f.write("invalid json")

    # Log a new execution
    run_summary.log_function_execution(run_directory, test_function, memo_uri, status="invoked")

    new_log_files = list(run_directory.glob("*.json"))
    assert len(new_log_files) == 2

    # Find the new valid log file
    new_log_file = next(file for file in new_log_files if file != log_file)
    with new_log_file.open() as f:
        log_data: run_summary.LogEntry = json.load(f)

    assert log_data["function_name"] == "tests.test_mops.unit.pure.test_run_summary:test_function"
    assert log_data["memo_uri"] == memo_uri
    assert log_data["status"] == "invoked"
    assert datetime.fromisoformat(log_data["timestamp"])


@pytest.mark.parametrize(
    "memo_uri, expected",
    [
        (
            "adls://thdsscratch/tmp/mops2-mpf/examples/__main__--find_in_file/MarchCupRisky.C4_yzRq1cztNXeuiwjOUM_Gotnq6aTwcxxticlE",
            "__main__find_in_file_MarchCupRiskyC4_yzRq1cztNXeuiwjOUM_Gotnq6aTwcxxticlE",
        ),
        (
            "adls://thdsscratch/tmp/mops2-mpf/Cheick-Berthe/2024-06-05T14:46:16-p7701/__main__--mul2/GriefAimToken.eQr9bcJeS_yka9pcmrfQuf6IBGiSOBB-husCdqI",
            "__main__mul2_GriefAimTokeneQr9bcJeS_yka9pcmrfQuf6IBGiSOBBhusCdqI",
        ),
        ("adls://pipeline-id/complex/function-id", "complex_functionid"),
    ],
)
def test_extract_and_format_part(memo_uri: str, expected: str) -> None:
    assert run_summary._extract_and_format_part(memo_uri) == expected
