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
import sys
import time
from timeit import default_timer

from thds.core.log import getLogger

from ....__about__ import __version__
from .. import metadata
from .runner_registry import run_named_entry_handler

logger = getLogger(__name__)


def main() -> None:
    """Routes the top level remote function call in a new process."""
    start = default_timer()
    start_timestamp = time.time()
    remote_proc_log = f"Entering remote process {os.getpid()} with installed mops version {__version__}"
    if remote_code_version := metadata.get_remote_code_version(""):
        remote_proc_log += f" and remote code version {remote_code_version}"
    logger.info(remote_proc_log)
    logger.info("mops full sys.argv: " + " ".join(sys.argv))
    parser = argparse.ArgumentParser(description="Unknown arguments will be passed to the named runner.")
    parser.add_argument(
        "runner_name",
        help="Name of a known remote runner that can handle the rest of the arguments",
    )
    # TODO potentially allow things like logger context to be passed in as -- arguments
    args, unknown = parser.parse_known_args()
    run_named_entry_handler(args.runner_name, *unknown)
    logger.info(
        f"Exiting remote process {os.getpid()} after {(default_timer() - start)/60:.2f} minutes"
        + metadata.format_end_of_run_times(start_timestamp, unknown)
    )


if __name__ == "__main__":
    main()  # pragma: no cover
