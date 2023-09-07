"""In many cases, we want to put files on ADLS and keep them there -
i.e., the orchestrator never wants to download the file - instead it
wants to have a way of referencing it locally and be able to hand it
off to another remote invocation in the future.

If you only wanted to pass these references around in-memory, it would
be trivial. You'd simply need to make your own type that satisfied the
purpose, and use it directly in your code. This module satisfies a
different purpose - the case where:

1) You want to run your process locally sometimes (i.e. remove a
   use_runner decorator from a function), and in that case you do
   _not_ want to have to upload and download the file.  Because of
   this, it must be possible to _open_ the file 'normally' within
   Python code.

2) You also want to be able to store these references between process
   runs.  This enables state to persist beyond a process lifetime,
   just like files, but without requiring that state to be fully
   present locally; instead, the file is a pointer to the remote file
   which can be (under certain circumstances) properly processed by
   Python.

This may seem like a contrived situation, but Demand Forecast could
really benefit from this, and ideally our solution will solve for a
somewhat broader range of use cases.

"""
# This source code no longer has any actual dependencies on the rest
# of mops.  It is intended to remain that way, so that it can be an
# abstraction that is obviously 'clean'.
import os
import shutil
import tempfile
import typing as ty
from pathlib import Path

from thds.core.log import getLogger

from .up_down import (
    Downloader,
    NamedDownloader,
    NamedUriUploader,
    Serialized,
    StrOrPath,
    Uploader,
    reify_downloader,
    reify_uploader,
)

logger = getLogger(__name__)


MAX_FILENAME_LEN = 150
# conservative cap on the max temp filename length


class DestFile:
    """A write-only file that provides local open semantics but may be uploaded after write.

    The file MUST NOT be accessed except via its context manager
    (`with dest_file as writable_filepath:`).

    Also, the file MUST NOT be read, before or after write.  In order
    to 'read' the previously-written contents of a DestFile, you must
    first convert it into a SrcFile.

    DestFiles additionally represent enforced atomicity, as the
    initial file write will happen in a temporary file, and only once
    the context is exited will the resulting file be moved to its
    final or remote destination.

    If the computation is not running remotely, it will instead place
    the file in a local location.

    If the computation _does_ run remotely, a descriptive placeholder
    file will be placed at the local destination immediately upon
    return from the remote process. DestFiles not returned as part of
    the function arguments will _not_ have placeholders
    written. Therefore, most usage of DestFile should be as _both_ a
    function argument _and_ (part of) a return value.

    Should usually be initially constructed in a local orchestrator
    process. May be used in the same local process or in a remote
    process.

    """

    def __init__(self, uploader: ty.Union[Uploader, NamedUriUploader], local_filename: StrOrPath):
        reify_uploader(uploader)  # test that it can be reified.
        self._uploader = uploader
        self._local_filename = os.fspath(local_filename)
        if not local_filename:
            self._mark_as_remote()
            # with no local filename, that means we are remote-only
            # and _must_ upload the file at __exit__.

        # we are careful throughout this class (and SrcFile) to
        # refrain from storing things as Paths, because it is likely
        # that these objects will pass through a `use_runner`
        # function boundary at some point (this is what they're
        # designed for!)  and when that happens, if something is out
        # of whack, we do not want to see the internal Path object
        # pickled and uploaded/downloaded.
        self._serialized_remote_pointer = Serialized("")

    def _force_serialization(self) -> None:
        """Only meaningful to call this once the file has been written
        to, and can only meaningfully be called in the same execution
        context where the file was written.
        """
        if not self._serialized_remote_pointer:
            logger.warning(f"Uploading file {self._local_filename} so that we have its serialization")
            assert os.path.exists(self._local_filename), "Only valid for locally-present DestFiles"
            self._serialized_remote_pointer = reify_uploader(self._uploader)(self._local_filename)

    def _is_remote(self) -> bool:
        return hasattr(self, "_marked_remote")

    def _mark_as_remote(self) -> None:
        self._marked_remote = True

    def __enter__(self) -> Path:
        """Return a temporary local file Path that may be used for opening or moving to.

        When the context exits, this temporary file will be uploaded if remote,
        or moved to the local destination if not remote.
        """
        reify_uploader(self._uploader)
        _fh, self._temp_dest_filepath = tempfile.mkstemp(
            suffix=os.path.basename(self._local_filename)[:MAX_FILENAME_LEN]
        )
        # we use mkstemp instead of TemporaryFile because we don't want it to auto-delete on close,
        os.close(_fh)  # and we specifically don't want this open ourselves.
        return Path(self._temp_dest_filepath)

    def __exit__(self, exc_type, _exc_val, _exc_tb):
        """Upload locally-created file if remote - otherwise move to local destination directly.

        It's critical that this get run within the is_remote scope if
        running remotely.  To ensure this, you should enter the
        context when you write to the file and then close it as soon
        as you are done writing it for the last time.
        """
        if exc_type is not None:
            # do not upload - this constitutes a failure
            os.remove(self._temp_dest_filepath)
            del self._temp_dest_filepath
            return

        if self._is_remote():
            try:
                self._serialized_remote_pointer = reify_uploader(self._uploader)(
                    self._temp_dest_filepath
                )
                logger.info(f"Uploaded DestFile to {self._serialized_remote_pointer}")
            finally:
                os.remove(self._temp_dest_filepath)
        else:
            if self._local_filename:
                logger.info(f"Moving temp file to dest file on local system: {self._local_filename}")
                shutil.move(os.fspath(self._temp_dest_filepath), self._local_filename)
                # we use shutil.move instead of os.rename because it will
                # transparently handle moving across different filesystems
                # if for some reason the temporary file is created on a
                # different FS.
            else:
                logger.warning("Using temp file path because no local filename was provided")
                # this should mostly only ever happen in rare cases with a remotely-created
                # DestFile that is being tested outside a remote function context.
                self._local_filename = self._temp_dest_filepath
        del self._temp_dest_filepath
        # we don't want this meaningless file path to exist later

    def __str__(self) -> str:
        return self._local_filename

    def __fspath__(self) -> str:
        return self._local_filename


# TODO 2.0 SrcFile is a monstrously large class that I'd love to see
# simplified.  It seems plausible that it could be split into two
# completely separate classes - one that represents a
# locally-available file, and another that represents a file that must
# be downloaded to be used. The former could be 'replaced' by the
# latter upon upload.
#
# Unfortunately, it also seems like that to perform this split,
# we'd need to make changes that would be backward-incompatible with
# existing serialized SrcFiles, which would break memoization.

# TODO don't store any local paths in SrcFile
# across serialization boundaries. The only 'determinstic' part of the object
# is the remote representation, so it would be nice to make memoization
# work regardless of fully-qualified local paths.


class SrcFile:
    """A read-only file that uses local open semantics, but may until first access be located remotely.

    The file MUST NOT be accessed except within its context manager
    (`with src_file as readable_filepath:`).

    Must always be constructed on a local orchestrator process, but
    should not be accessed after 'return' from a remote process.

    `serialized_remote_pointer` AND/OR `local_path` must be provided
    and be non-empty. If the latter is provided, the local file MUST
    already exist on the local filesystem.

    Uploader should likely contain its own process-wide lock, to avoid
    re-upload in cases of intra-process concurrency.
    """

    def __init__(
        self,
        downloader: ty.Union[Downloader, NamedDownloader],
        serialized_remote_pointer: Serialized = Serialized(""),  # noqa: B008
        local_path: StrOrPath = "",
        uploader: ty.Union[NamedUriUploader, Uploader, None] = None,
    ):
        """Must only be called on a local orchestrator process."""
        # immediately determine whether this represents a fully-present local source
        # file, a placeholder pointing to a remote file, or an already-serialized remote pointer
        self._local_filename = os.fspath(local_path)
        self._serialized_remote_pointer = serialized_remote_pointer
        if self._local_filename and not os.path.exists(self._local_filename):
            raise FileNotFoundError(
                "Source must exist at construction time on local orchestrator: " + self._local_filename
            )
        if not self._local_filename:
            if not self._serialized_remote_pointer:
                raise ValueError("Must provide either local path or serialized remote pointer")
        self._uploader = uploader
        if uploader:
            reify_uploader(uploader)
        if not self._serialized_remote_pointer:
            assert (
                self._uploader
            ), "If file is not a remote file pointer, uploader must be provided as well"
        self._downloader = downloader
        reify_downloader(downloader)  # just to test that it can be reified.
        self._temp_src_filepath: str = ""
        self._entrance_count = 0

    def _upload_if_not_already_remote(self):
        """Upload happens here - but not until entering `use_runner`.

        Called by framework code in the local orchestrator process at
        the beginning of `use_runner`, before handoff to the Runner.

        Once uploaded, the file can no longer be used from its local filename.
        """
        if self._serialized_remote_pointer:
            return  # it's already available remotely
        assert self._uploader, "No serialized remote pointer is present and no uploader is available"
        logger.info(f"Local file {self._local_filename} requires upload")
        self._serialized_remote_pointer = reify_uploader(self._uploader)(self._local_filename)
        self._local_filename = ""
        self._uploader = None

    def _mark_as_remote(self) -> None:
        self._marked_remote = True

    def _is_remote(self) -> bool:
        return hasattr(self, "_marked_remote")

    def __enter__(self) -> Path:
        """Return a local path suitable for opening and reading.

        If located remotely, will be downloaded to a temporary file
        prior to return of a temporary path.

        If located locally, the local path will be provided directly.

        Writes to this path are undefined behavior.
        """
        self._entrance_count += 1
        if self._temp_src_filepath:
            return Path(
                self._temp_src_filepath
            )  # pragma: nocover  # cannot 'see' test b/c it runs remotely

        if self._is_remote() or self._serialized_remote_pointer:
            _fh, self._temp_src_filepath = tempfile.mkstemp(
                suffix=os.path.basename(self._local_filename)[:MAX_FILENAME_LEN]
            )
            os.close(_fh)  # we don't want the file to be open
            reify_downloader(self._downloader)(self._serialized_remote_pointer, self._temp_src_filepath)
            return Path(self._temp_src_filepath)

        assert os.path.exists(
            self._local_filename
        ), f"The source represented does not exist on the current filesystem: {self._local_filename}"
        return Path(self._local_filename)

    def __exit__(self, *_args, **_kwargs):
        """Clean up temporary file if one exists."""
        self._entrance_count -= 1
        if self._temp_src_filepath and not self._entrance_count:
            assert (
                self._is_remote() or self._serialized_remote_pointer
            ), "No temp file should have been created in this context."
            try:
                os.remove(self._temp_src_filepath)
            except FileNotFoundError:
                pass
            self._temp_src_filepath = ""
