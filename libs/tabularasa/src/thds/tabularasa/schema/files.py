import csv
import datetime
import os
from enum import Enum
from pathlib import Path
from typing import Dict, List, Literal, Optional, Set, Tuple, Union

import pkg_resources
from pydantic import AnyUrl, BaseModel, Extra, Field

from .util import DashedIdentifier, DocumentedMixin, DottedIdentifier, HexStr, PathStr


class CSVQuotingConvention(Enum):
    QUOTE_ALL = "quote_all"
    QUOTE_NONE = "quote_none"
    QUOTE_NONNUMERIC = "quote_nonnumeric"
    QUOTE_MINIMAL = "quote_minimal"


UpdateFrequency = Literal["Yearly", "Quarterly", "Monthly", "Biannual"]


def quarter(date: datetime.date) -> int:
    return (date.month - 1) // 3 + 1


def half(date: datetime.date) -> int:
    return (date.month - 1) // 6 + 1


def _get_tail(freq: UpdateFrequency, date: datetime.date) -> Tuple[int, ...]:
    if freq == "Yearly":
        return ()
    if freq == "Quarterly":
        return (quarter(date),)
    if freq == "Monthly":
        return (date.month,)
    return (half(date),)


def _date_tuple(date: datetime.date, freq: UpdateFrequency) -> Tuple[int, ...]:
    return (date.year, *_get_tail(freq, date))


current_date = datetime.date.today()


class FileSourceMixin(BaseModel, extra=Extra.forbid):
    authority: Optional[str] = None
    url: Optional[AnyUrl] = None
    landing_page: Optional[AnyUrl] = None
    last_checked: Optional[datetime.date] = None
    last_updated: Optional[datetime.date] = None
    update_frequency: Optional[UpdateFrequency] = None
    is_open_access: Optional[bool] = None
    doc: Optional[str] = None

    def needs_update(self, current_date: datetime.date) -> bool:
        if self.update_frequency is not None:
            if self.last_updated is None:
                return True
            return _date_tuple(current_date, self.update_frequency) > _date_tuple(
                self.last_updated, self.update_frequency
            )
        return False


class LocalFileSourceMixin(FileSourceMixin):
    filename: PathStr
    package: Optional[DottedIdentifier] = None
    encoding: Optional[str] = None

    @property
    def full_path(self) -> Path:
        return Path(
            self.filename
            if self.package is None
            else pkg_resources.resource_filename(self.package, self.filename)
        )

    @property
    def file_handle(self):
        if self.package is None:
            return open(self.filename, "rb")
        else:
            if pkg_resources.resource_isdir(self.package, self.filename):
                raise IsADirectoryError(pkg_resources.resource_filename(self.package, self.filename))
            return pkg_resources.resource_stream(self.package, self.filename)

    @property
    def is_dir(self) -> bool:
        if self.package is None:
            return os.path.isdir(self.filename)
        else:
            return pkg_resources.resource_isdir(self.package, self.filename)


class TabularFileSource(DocumentedMixin, LocalFileSourceMixin):
    delimiter: Optional[str] = csv.excel.delimiter
    quotechar: Optional[str] = csv.excel.quotechar
    escapechar: Optional[str] = csv.excel.escapechar
    doublequote: Optional[bool] = csv.excel.doublequote
    skipinitialspace: Optional[bool] = csv.excel.skipinitialspace
    lineterminator: Optional[str] = csv.excel.lineterminator
    skiprows: Optional[int] = None
    quoting: Optional[CSVQuotingConvention] = CSVQuotingConvention.QUOTE_MINIMAL
    package: Optional[DottedIdentifier] = None
    # Fairly conservative choice - only empty string is treated as explicitly null,
    # and only on nullable columns
    na_values: Optional[Set[str]] = Field(default_factory=lambda: {""})

    @property
    def csv_dialect(self) -> csv.Dialect:
        # This is ugly but required for flexibility when using pandas.read_csv.
        # The reason is that while read_csv allows passing all the attributes of a csv.Dialect,
        # it does _not_ allow passing multi-char lineterminators (as are present in csv.excel as '\r\n'
        # and this is often required in practice). The simple-seeming thing then is to pass the excel
        # dialect and let the keyword args override it, but pandas overrides in the _opposite_ direction:
        # the dialect overrides the keyword args. So in order to inject e.g. different delimiters or
        # quoting conventions while keeping the '\r\n' lineterminator of the excel dialect, we have to
        # build a new dialect object here.
        kwargs = self.csv_reader_kwargs

        if all(v == getattr(csv.excel, k) for k, v in kwargs.items()):
            return csv.excel()
        else:
            dialect_name = "csv_dialect%d" % abs(hash(tuple(sorted(kwargs.items()))))
            try:
                dialect = csv.get_dialect(dialect_name)
            except Exception:
                dialect_cls = type(dialect_name, (csv.excel,), kwargs)
                csv.register_dialect(dialect_name, dialect_cls)
                dialect = dialect_cls()

            return dialect

    @property
    def csv_reader_kwargs(self) -> Dict[str, Union[str, int, bool]]:
        kw: Dict[str, Union[str, int, bool]] = {}
        if self.quoting is not None:
            kw.update(quoting=getattr(csv, self.quoting.name))

        for name in [
            "delimiter",
            "quotechar",
            "escapechar",
            "doublequote",
            "skipinitialspace",
            "lineterminator",
            "skiprows",
        ]:
            value = self.__dict__.get(name)
            if value is not None:
                kw[name] = value

        return kw


class VersionControlledPath(BaseModel, extra=Extra.forbid):
    name: PathStr
    md5: Optional[HexStr] = None


class ADLSDataSpec(FileSourceMixin):
    adls_account: DashedIdentifier
    adls_filesystem: DashedIdentifier
    paths: List[VersionControlledPath] = Field(min_items=1)
    ordered: bool = False


class LocalDataSpec(LocalFileSourceMixin):
    order: List[PathStr] = Field(default_factory=list, unique_items=True)
    package: Optional[DottedIdentifier] = None

    def list_dir(self):
        if self.package is None:
            return os.listdir(self.filename)
        else:
            return pkg_resources.resource_listdir(self.package, self.filename)

    @property
    def all_data_specs(self) -> List["LocalDataSpec"]:
        if self.is_dir:
            return [
                LocalDataSpec(package=self.package, filename=os.path.join(self.filename, filename))
                for filename in self.list_dir()
            ]
        else:
            return [self]

    @property
    def ordered_data_specs(self) -> List["LocalDataSpec"]:
        if not self.order:
            raise AttributeError("No `order` is set - `ordered_data_specs` are not available")
        spec_order = {os.path.basename(spec.filename): spec for spec in self.all_data_specs}
        if set(spec_order.keys()) != set(self.order):
            raise ValueError(
                f"`order` does not match the file names in the spec: {set(self.order)} != {set(spec_order.keys())}"
            )
        return [spec_order[name] for name in self.order]


class RemoteBlobStoreSpec(BaseModel, extra=Extra.forbid):
    adls_account: DashedIdentifier
    adls_filesystem: DashedIdentifier
    path: PathStr

    def data_spec(self, md5: str, extension: Optional[str] = None) -> ADLSDataSpec:
        ext = extension or ""
        return ADLSDataSpec(
            adls_account=self.adls_account,
            adls_filesystem=self.adls_filesystem,
            paths=[VersionControlledPath(name=f"{self.path.rstrip('/')}/{md5}{ext}", md5=md5)],
        )
