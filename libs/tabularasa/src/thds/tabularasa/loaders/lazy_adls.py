"""Make SQLite Loaders that lazily load their source from pre-built ADLS paths.

The download will only occur once, and each thread will get its own
SQLite connection, as is proper.
"""
import typing as ty
from pathlib import Path

from thds.adls import AdlsFqn
from thds.core.lazy import Lazy, ThreadLocalLazy

from ..data_dependencies.cached_installer import make_caching_installer
from .sqlite_util import AttrsSQLiteDatabase

L = ty.TypeVar("L")


def make_lazy_attrs_sqlite_loader(
    mk_loader: ty.Callable[[AttrsSQLiteDatabase], L],
    db_installer: ty.Callable[[], Path],
    mmap_size: int = 2**24,
) -> ThreadLocalLazy[L]:
    one_time_db_install = Lazy(db_installer)

    def make_loader():
        # the DB installer is made lazy so that multiple threads
        # competing to install it will only install it once.
        return mk_loader(AttrsSQLiteDatabase(None, one_time_db_install(), mmap_size=mmap_size))

    return ThreadLocalLazy(make_loader)


def lazy_attrs_sqlite_loader_maker(
    mk_loader: ty.Callable[[AttrsSQLiteDatabase], L],
    default_mmap_size: int = 2**24,
) -> ty.Callable[[AdlsFqn, str], ThreadLocalLazy[L]]:
    def make_loader(fqn: AdlsFqn, md5b64: str, mmap_size: int = -1) -> ThreadLocalLazy[L]:
        return make_lazy_attrs_sqlite_loader(
            mk_loader,
            make_caching_installer(fqn, md5b64),
            mmap_size if mmap_size > -1 else default_mmap_size,
        )

    return make_loader
