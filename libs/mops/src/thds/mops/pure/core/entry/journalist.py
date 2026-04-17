"""Optional resource-usage monitoring for mops remote entry processes.

When `mops.journalist.log_interval` is configured to a positive float (seconds) - most
easily by setting the `MOPS_JOURNALIST_LOG_INTERVAL` env var via a k8s shim builder or
the project's Dockerfile - the remote entry wraps `run_named_entry_handler` in a
`thds.core.journalist.Journalist` that samples RSS, CPU, and network IO across the
process tree and logs at the configured interval.

The env var is popped on activation so subprocess-shim children - which inherit env -
do not re-activate the journalist in their own process. In-process re-entry is
prevented by `Journalist` itself. K8s child pods receive their env from the shim builder
or Dockerfile rather than inheriting from this process, so the pop does not prevent
them from running their own journalist.
"""

import contextlib
import os
import typing as ty

from thds.core import config
from thds.core.journalist import Journalist
from thds.core.log import getLogger

from ..memo import parse_memo_uri

logger = getLogger(__name__)

JOURNALIST_INTERVAL_ENV = "MOPS_JOURNALIST_LOG_INTERVAL"


def _parse_log_interval(value: ty.Any) -> float:
    if isinstance(value, (int, float)):
        return float(value)

    try:
        return float(value)
    except (TypeError, ValueError):
        logger.warning(
            f"Ignoring non-numeric value {value!r} for mops.journalist.log_interval;"
            " journalist disabled."
        )
        return 0.0


LOG_INTERVAL = config.item("mops.journalist.log_interval", 0.0, parse=_parse_log_interval)


def _label(runner_name: str, entry_args: ty.Sequence[str]) -> str:
    if not entry_args:
        return runner_name

    try:
        parts = parse_memo_uri(entry_args[0], runner_name)
    except (ValueError, AssertionError):
        return runner_name

    last_module = parts.function_module.rsplit(".", 1)[-1]
    return f"{last_module}.{parts.function_name}"


@contextlib.contextmanager
def maybe_journalist(runner_name: str, entry_args: ty.Sequence[str]) -> ty.Iterator[None]:
    interval = LOG_INTERVAL()
    if interval <= 0:
        yield
        return

    # Subprocess shims inherit env; pop so the child does not re-activate.
    # (In-process re-entry is handled by Journalist itself.)
    os.environ.pop(JOURNALIST_INTERVAL_ENV, None)

    with Journalist(_label(runner_name, entry_args), interval=interval):
        yield
