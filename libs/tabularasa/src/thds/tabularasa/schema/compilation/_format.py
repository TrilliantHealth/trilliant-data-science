import logging
import os.path
import subprocess
import tempfile
from functools import lru_cache
from warnings import warn


@lru_cache
def _ruff_available() -> bool:
    try:
        subprocess.run(["ruff", "--version"], capture_output=True, check=True)
        return True
    except (FileNotFoundError, subprocess.CalledProcessError):
        warn(
            "`ruff` is unavailable; generated python code will not be auto-formatted. "
            "Specify the 'cli' extra to ensure this dependency is present."
        )
        return False


def autoformat(py_code: str) -> str:
    _LOGGER = logging.getLogger(__name__)
    if not _ruff_available():
        return py_code

    try:
        with tempfile.TemporaryDirectory() as d:
            outfile = os.path.join(d, "tmp.py")
            with open(outfile, "w") as f:
                f.write(py_code)

            _LOGGER.info("Applying `ruff` import sorting to auto-generated code")
            subprocess.run(
                ["ruff", "check", "--select", "I", "--fix", outfile],
                capture_output=True,
                check=False,
            )

            _LOGGER.info("Applying `ruff` formatting to auto-generated code")
            subprocess.run(["ruff", "format", outfile], capture_output=True, check=True)

            with open(outfile, "r") as f_:
                py_code = f_.read()

        return py_code
    except Exception as ex:
        print(f"{repr(ex)} when attempting to format code:")
        print(py_code)
        raise
