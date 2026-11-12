import asyncio
import contextlib
import os
import subprocess
import threading

from thds.core import cache, config, cpus, log

MAX_PROCESSES = config.item("max_processes", default=None, parse=config.maybe(int))  # unset: no cap
logger = log.getLogger(__name__)


@cache.locking
def restrict_usage() -> dict:
    num_cpus = cpus.available_cpu_count()

    env = dict(os.environ)
    if "AZCOPY_BUFFER_GB" not in os.environ:
        likely_mem_gb_available = num_cpus * 4  # assume 4 GB per CPU core is available
        # o3 suggested 15% of the total available memory...
        env["AZCOPY_BUFFER_GB"] = str(likely_mem_gb_available * 0.15)
    if "AZCOPY_CONCURRENCY" not in os.environ:
        env["AZCOPY_CONCURRENCY"] = str(int(num_cpus * 2))

    logger.info(
        "AZCOPY_BUFFER_GB == %s and AZCOPY_CONCURRENCY == %s",
        env["AZCOPY_BUFFER_GB"],
        env["AZCOPY_CONCURRENCY"],
    )
    return env


@cache.locking
def _process_slots() -> threading.BoundedSemaphore | None:
    max_processes = MAX_PROCESSES()  # read at first use, then fixed
    return None if max_processes is None else threading.BoundedSemaphore(max_processes)


def acquire_process_slot() -> bool:
    """Never blocks. On False, do not start azcopy; use the Python SDK instead.

    restrict_usage sizes each azcopy process as if it ran alone, so many parallel
    transfers, each with its own azcopy process, can need many times that memory.
    MAX_PROCESSES, when set, limits how many azcopy processes run at once in this Python
    process. Release each acquired slot with release_process_slot or
    release_process_slot_async, which stop the process first.
    """
    slots = _process_slots()
    if slots is None or slots.acquire(blocking=False):
        return True

    logger.debug("All %d azcopy process slots are in use", MAX_PROCESSES())
    return False


def stop_process(process: subprocess.Popen) -> None:
    """Kill the process if it still runs, and reap it. A no-op after a normal exit."""
    if process.poll() is None:
        process.kill()
        process.wait()


def _free_slot() -> None:
    slots = _process_slots()
    if slots is not None:
        slots.release()


def release_process_slot(process: subprocess.Popen | None = None) -> None:
    """Stop the slot's process, if any, so that a free slot never has a live azcopy."""
    try:
        if process is not None:
            stop_process(process)
    finally:
        _free_slot()


async def release_process_slot_async(process: asyncio.subprocess.Process | None) -> None:
    """As release_process_slot. A cancellation that arrives while the killed process is
    reaped does not cut the wait short: it is raised again once the slot is free.
    """
    cancellation: asyncio.CancelledError | None = None
    try:
        if process is not None and process.returncode is None:
            with contextlib.suppress(ProcessLookupError):  # it exited after the check
                process.kill()
            while process.returncode is None:
                try:
                    await process.wait()
                except asyncio.CancelledError as cancelled:
                    cancellation = cancelled  # the process has SIGKILL, so the wait ends soon
    finally:
        _free_slot()
    if cancellation is not None:
        raise cancellation
