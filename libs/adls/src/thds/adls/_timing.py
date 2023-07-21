import contextlib
import typing as ty
from logging import LoggerAdapter
from timeit import default_timer

from thds.core import log

EmitRate = ty.Callable[[int], None]
logger = log.getLogger(__name__)
_1MB = 2**20


@contextlib.contextmanager
def download_timer(
    src: ty.Any,
    dest: str = "",
    long_transfer_s: float = 3.0,
    logger: LoggerAdapter = logger,
    known_size: int = 0,
) -> ty.Iterator[EmitRate]:
    dest_s = f" to {dest}" if dest else ""
    if known_size > 100 * _1MB:
        log = logger.info
        size_s = f" of size {known_size:,} bytes"
    else:
        log = logger.debug
        size_s = ""
    log(f"Downloading {src}{dest_s}{size_s}")

    start = default_timer()
    elapsed = None

    def emit_download_rate(n_bytes: int) -> None:
        nonlocal elapsed
        size_s = f" {n_bytes:,} bytes" if n_bytes else ""
        elapsed = default_timer() - start
        rate_s = f" at {int(n_bytes/_1MB/elapsed):,.1f} MiB/s"
        log = logger.info if elapsed > long_transfer_s else logger.debug
        log(f"Downloaded{size_s} in {elapsed:.1f}s{rate_s} from {src}{dest_s}")

    yield emit_download_rate

    if elapsed is None:
        # if you forget to emit the number of bytes, then we'll at least log the time for you.
        emit_download_rate(0)


@contextlib.contextmanager
def upload_timer(
    dest: ty.Any,
    src: str = "",
    long_transfer_s: float = 3.0,
    logger: LoggerAdapter = logger,
    large_upload_size: int = 0,
) -> ty.Iterator[EmitRate]:
    src_s = f" from {src}" if src else ""
    large_upload_size_s = f" {large_upload_size:,} bytes" if large_upload_size else ""
    log = logger.info if large_upload_size else logger.debug
    log(f"Uploading{large_upload_size_s}{src_s} to {dest}")

    start = default_timer()
    elapsed = None

    def emit_upload_rate(n_bytes: int = 0) -> None:
        nonlocal elapsed
        size_s = f"{n_bytes:,} bytes " if n_bytes else ""
        elapsed = default_timer() - start
        rate_s = f"at {int(n_bytes/_1MB/elapsed):,.1f} MiB/sec " if n_bytes else ""
        log = logger.info if elapsed > long_transfer_s else logger.debug
        log(f"Uploaded {size_s}in {elapsed:.1f}s {rate_s}{src_s}to {dest}")

    yield emit_upload_rate

    if elapsed is None:
        # if you forget to emit the number of bytes, then we'll at least log the time for you.
        emit_upload_rate(0)
