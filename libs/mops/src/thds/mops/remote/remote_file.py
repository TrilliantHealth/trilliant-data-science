"""In many cases, we want to put files on ADLS and keep them there -
i.e., the orchestrator never wants to download the file - instead it
wants to have a way of referencing it locally and be able to hand it
off to another remote invocation in the future.

If you only wanted to pass these references around in-memory, it would
be trivial. You'd simply need to make your own type that satisfied the
purpose, and use it directly in your code. This module satisfies a
different purpose - the case where:

1) You want to run your process locally sometimes (i.e. remove a
   pure_remote decorator from a function), and in that case you do
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
import io
import os
import pickle  # only used for recursive visiting
import shutil
import tempfile
import typing as ty
from pathlib import Path

from thds.core.log import getLogger

from ._root import is_remote
from .types import T

logger = getLogger(__name__)


Serialized = ty.NewType("Serialized", str)
StrOrPath = ty.Union[str, os.PathLike]
Uploader = ty.Callable[[StrOrPath], Serialized]
# uploads local file and returns serialized representation of remote file location
Downloader = ty.Callable[[Serialized, StrOrPath], None]
# interprets the serialized string as a remote file location and downloads it to the provided local path


class DestFile:
    """A write-only file that provides local open semantics but may be uploaded after write.

    The file MUST NOT be accessed except via its context manager
    (`with dest_file as writable_filepath:`).

    They additionally represent enforced atomicity, as the initial
    file write will happen in a temporary file, and only once the
    context is exited will the resulting file be moved to its final or
    remote destination.

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

    def __init__(self, uploader: Uploader, str_or_path: StrOrPath):
        self._uploader = uploader
        self._local_filename = os.fspath(str_or_path)
        # we are careful throughout this class (and SrcFile) to
        # refrain from storing things as Paths, because it is likely
        # that these objects will pass through a `pure_remote`
        # function boundary at some point (this is what they're
        # designed for!)  and when that happens, if something is out
        # of whack, we do not want to see the internal Path object
        # pickled and uploaded/downloaded.
        self._serialized_remote_pointer = Serialized("")

    def __enter__(self) -> Path:
        """Return a temporary local file Path that may be used for opening or moving to.

        When the context exits, this temporary file will be uploaded if remote,
        or moved to the local destination if not remote.
        """
        _fh, self._temp_dest_filepath = tempfile.mkstemp(suffix=os.path.basename(self._local_filename))
        # we use mkstemp instead of TemporaryFile because we don't want it to auto-delete on close,
        os.close(_fh)  # and we specifically don't want this open ourselves.
        return Path(self._temp_dest_filepath)

    def __exit__(self, *_args, **_kwargs):
        """Upload locally-created file if remote - otherwise move to local destination directly.

        It's critical that this get run within the is_remote scope if
        running remotely.  To ensure this, you should enter the
        context when you write to the file and then close it as soon
        as you are done writing it for the last time.
        """
        if is_remote():
            try:
                self._serialized_remote_pointer = self._uploader(self._temp_dest_filepath)
                logger.info(f"Uploaded DestFile to {self._serialized_remote_pointer}")
            finally:
                os.remove(self._temp_dest_filepath)
        else:
            logger.info(f"Moving temp file to dest file on local system: {self._local_filename}")
            shutil.move(os.fspath(self._temp_dest_filepath), self._local_filename)
            # we use shutil.move instead of os.rename because it will transparently handle
            # moving across different filesystems if for some reason the temporary file is created on a different FS.
        del self._temp_dest_filepath  # we don't want this meaningless file path to exist later

    def _write_serialized_remote_to_placeholder(self):
        """For use by framework code in the local orchestrator process.

        Called during _return_ from `pure_remote`.
        """
        if not self._serialized_remote_pointer:
            # this means that no file was written remotely.
            # therefore, no file should be written locally either
            if not Path(self._local_filename).exists():
                logger.warning(
                    f"DestFile {self._local_filename} with empty serialization "
                    "returned from pure_remote-decorated function."
                )
            else:
                logger.debug("Local dest file being returned without modification")
            return
        logger.info(f"Writing remote file pointer to local path {self._local_filename}")
        Path(self._local_filename).parent.mkdir(exist_ok=True, parents=True)
        with open(self._local_filename, "w") as f:
            f.write(self._serialized_remote_pointer)

    def __str__(self) -> str:
        return self._local_filename

    def __fspath__(self) -> str:
        return self._local_filename


class SrcFile:
    """A read-only file that uses local open semantics, but may until first access be located remotely.

    The file MUST NOT be accessed except within its context manager
    (`with src_file as readable_filepath:`).

    Must always be constructed on a local orchestrator process, but
    should not be accessed after 'return' from a remote process.

    `serialized_remote_pointer` AND/OR `local_path` must be provided
    and be non-empty. If the latter is provided, the local file MUST
    already exist on the local filesystem.
    """

    def __init__(
        self,
        downloader: Downloader,
        serialized_remote_pointer: Serialized = Serialized(""),  # noqa: B008
        local_path: StrOrPath = "",
        uploader: ty.Optional[Uploader] = None,
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
            assert (
                self._serialized_remote_pointer
            ), "Must provide either local file or remote file pointer"
        self._uploader = uploader
        if self._local_filename:
            assert self._uploader, "If local file is present, uploader must be provided as well"
        self._downloader = downloader
        self._temp_src_filepath: str = ""
        self._entrance_count = 0

    def _upload_if_not_already_remote(self):
        """Upload happens here - but not until entering `pure_remote`.

        Called by framework code in the local orchestrator process at
        the beginning of `pure_remote`, before handoff to the Runner.
        """
        if self._serialized_remote_pointer:
            return  # it's already available remotely
        assert self._uploader, "No serialized remote pointer is present and no uploader is available"
        logger.info(f"Local file {self._local_filename} requires upload")
        self._serialized_remote_pointer = self._uploader(self._local_filename)

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

        if is_remote() or self._serialized_remote_pointer:
            _fh, self._temp_src_filepath = tempfile.mkstemp(
                suffix=os.path.basename(self._local_filename)
            )
            os.close(_fh)  # we don't want the file to be open
            self._downloader(self._serialized_remote_pointer, self._temp_src_filepath)
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
                is_remote() or self._serialized_remote_pointer
            ), "No temp file should have been created in this context."
            os.remove(self._temp_src_filepath)
            self._temp_src_filepath = ""


def _recursive_visit(visitor: ty.Callable[[ty.Any], bool], obj: ty.Any):
    """A hilarious abuse of pickle to do nearly limitless object recursion in Python for us.

    Because pure_remote depends on serializability, in general this
    will not cause significant limitations that would not have been
    incurred during serialization later. This is, however, purely an
    implementation detail, so it could be replaced with some other
    approach later.
    """

    class PickleVisit(pickle.Pickler):
        def __init__(self, file):
            super().__init__(file)

        def reducer_override(self, obj: ty.Any):
            visitor(obj)
            return NotImplemented

    PickleVisit(io.BytesIO()).dump(obj)


def trigger_dest_files_placeholder_write(rval: T) -> T:
    """Runner implementations making use of remote filesystems should
    call this after remote function execution.
    """

    def visitor(obj: ty.Any):
        if isinstance(obj, DestFile):
            obj._write_serialized_remote_to_placeholder()

    _recursive_visit(visitor, rval)  # type: ignore
    return rval


def trigger_src_files_upload(args, kwargs):
    """Runner implementations making use of remote filesystems should
    call this before remote function execution.
    """

    def visitor(obj: ty.Any):
        if isinstance(obj, SrcFile):
            obj._upload_if_not_already_remote()

    _recursive_visit(visitor, args)
    _recursive_visit(visitor, kwargs)
