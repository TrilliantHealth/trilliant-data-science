"""It's critical to keep this main function separate from everything
in `.core`, because `.core` needs to be fully imported _before_ the
remote function starts to run, otherwise you get infinite remote
recursion without ever returning a result, since the core module ends
up halfway imported and the effect of the _IS_CALLED_BY_RUNNER.set(True) line
gets erased when the final function module inevitably re-imports
`.core` when being dynamically looked up, and core is not yet
registered in sys.modules because it's still running `main`.

Ask me how long it took to figure out what was going on there...
"""
import argparse
import os

from thds.core.log import getLogger

from ..__about__ import __version__
from ._registry import main_handler
from ._root import _IS_CALLED_BY_RUNNER
from .temp import _REMOTE_TMP

logger = getLogger(__name__)


def run_main(*args: str):
    try:
        with _IS_CALLED_BY_RUNNER.set(True):
            main_handler(*args)
    finally:
        _REMOTE_TMP.cleanup()


def main():
    """Routes the top level remote function call in a remote process."""
    logger.info(f"Entering remote process {os.getpid()} with installed mops version {__version__}")
    parser = argparse.ArgumentParser(description="Unknown arguments will be passed to the named runner.")
    parser.add_argument(
        "remote_runner",
        help="Name of a known remote runner that can handle the rest of the arguments",
    )
    # TODO potentially allow things like logger context to be passed in as -- arguments
    args, unknown = parser.parse_known_args()
    run_main(args.remote_runner, *unknown)


if __name__ == "__main__":
    main()  # pragma: no cover
