import logging
import os.path
import tempfile
from functools import lru_cache
from typing import Any, Callable, List, Optional, Tuple
from warnings import warn


@lru_cache
def __autoformat_imports() -> Tuple[Optional[Any], Optional[Callable[[List[str]], int]]]:
    try:
        import black
    except ImportError:
        warn(
            "`black` is unavailable; generated python code will not be auto-formatted. "
            "Specify the 'cli' extra to ensure this dependency is present."
        )
        black = None  # type: ignore
    try:
        from isort.main import main as isort_main  # type: ignore
    except ImportError:
        warn(
            "`isort` is unavailable; imports in generated python code will not be automatically sorted. "
            "Specify the 'cli' extra to ensure this dependency is present."
        )
        isort_main = None  # type: ignore
    return black, isort_main  # type: ignore


def autoformat(py_code: str) -> str:
    _LOGGER = logging.getLogger(__name__)
    try:
        black, isort_main = __autoformat_imports()
        if black is not None:
            _LOGGER.info("Applying `black` formatting to auto-generated code")
            py_code = black.format_str(py_code, mode=black.FileMode())
        if isort_main is not None:
            _LOGGER.info("Applying `isort` formatting to auto-generated code")
            with tempfile.TemporaryDirectory() as d:
                outfile = os.path.join(d, "tmp.py")
                with open(outfile, "w") as f:
                    f.write(py_code)
                isort_main([outfile, "--profile", "black"])
                with open(outfile, "r") as f_:
                    py_code = f_.read()
        return py_code
    except Exception as ex:
        print(f"{repr(ex)} when attempting to format code:")
        print(py_code)
        raise
