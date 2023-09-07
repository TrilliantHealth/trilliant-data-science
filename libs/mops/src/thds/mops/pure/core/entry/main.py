"""It's critical to keep this main function separate from everything
in `.core`, because `.core` needs to be fully imported _before_ the
remote function starts to run, otherwise you get infinite remote
recursion without ever returning a result, since the core module ends
up halfway imported and the effect of the IS_INSIDE_RUNNER_ENTRY.set(True) line
gets erased when the final function module inevitably re-imports
`.core` when being dynamically looked up, and core is not yet
registered in sys.modules because it's still running `main`.

Ask me how long it took to figure out what was going on there...
"""
import argparse
import os

from thds.core.log import getLogger

from ....__about__ import __version__
from .runner_registry import run_named_entry_handler

logger = getLogger(__name__)


def main():
    """Routes the top level remote function call in a new process."""
    logger.info(f"Entering remote process {os.getpid()} with installed mops version {__version__}")
    parser = argparse.ArgumentParser(description="Unknown arguments will be passed to the named runner.")
    parser.add_argument(
        "entry_handler",
        help="Name of a known remote runner that can handle the rest of the arguments",
    )
    # TODO potentially allow things like logger context to be passed in as -- arguments
    args, unknown = parser.parse_known_args()
    run_named_entry_handler(args.entry_handler, *unknown)


if __name__ == "__main__":
    main()  # pragma: no cover
