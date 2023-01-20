import io
import os
import pickle
import typing as ty
from functools import partial

from typing_extensions import Protocol

from thds.core.log import getLogger
from thds.core.stack_context import StackContext

from ._adls import AdlsFileSystem, download_to, join, upload_to
from ._byos import BYOS
from ._once import Once
from ._paths import Downloader, PathPickler
from ._pickle import CallableUnpickler, Dumper

logger = getLogger(__name__)


class AdlsPathStream:
    """Send local Paths to ADLS and get them back again."""

    def __init__(self, fs: AdlsFileSystem, prefix: str):
        self.fs = fs
        self.prefix = prefix

    def local_to_remote(self, local_src: os.PathLike, key: str):
        """Return fully qualified remote information after put."""
        upload_to(self.fs, join(self.prefix, key), local_src)

    def get_downloader(self, remote_key: str) -> Downloader:
        return partial(download_to, self.fs, join(self.prefix, remote_key))


def make_dumper(fs: AdlsFileSystem, adls_prefix: str, once: Once, byos: BYOS) -> Dumper:
    adls_path_stream = AdlsPathStream(fs, adls_prefix)
    return Dumper([PathPickler(adls_path_stream, once), byos])


class ReadObject(Protocol):
    def __call__(self, __path: str, *, type_hint: str = "bytes") -> object:
        ...


def make_read_object(fs: AdlsFileSystem) -> ReadObject:
    def read_object(adls_path: str, type_hint: str = "bytes") -> object:
        return CallableUnpickler(io.BytesIO(fs.get_bytes(adls_path, type_hint=type_hint))).load()

    return read_object


class UnpickleFromAdls:
    def __init__(self, fs: AdlsFileSystem, path: str):
        self.fs = fs
        self.path = path
        self._cached = None

    def __call__(self) -> object:
        # i don't believe there's any need for thread safety here, since pickle won't use threads.
        if self._cached is None:
            self._cached = pickle.load(io.BytesIO(self.fs.get_bytes(self.path, "pickle")))
        return self._cached


class AdlsContext(ty.NamedTuple):
    fs: AdlsFileSystem
    prefix: str


ADLS_CONTEXT: StackContext[ty.Optional[AdlsContext]] = StackContext("adls-context", None)


def pickle_to_adls_prefix(object_key: str, obj: ty.Any) -> UnpickleFromAdls:
    context = ADLS_CONTEXT()
    assert context, "Must have set an ADLS context"

    with io.BytesIO() as bio:
        pickle.dump(obj, bio)
        bio.seek(0)
        adls_path = join(context.prefix, object_key)
        context.fs.put_bytes(adls_path, bio)

    return UnpickleFromAdls(context.fs, adls_path)


class NamedAdlsPickler:
    """Name must be globally unique."""

    def __init__(self, name: str):
        self.name = name

    def __call__(self, obj: ty.Any):
        return pickle_to_adls_prefix(self.name, obj)
