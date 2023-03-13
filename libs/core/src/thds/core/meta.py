import json
import os
import re
import subprocess as sp
import typing as ty
from datetime import datetime, timezone
from functools import lru_cache
from getpass import getuser
from importlib.metadata import PackageNotFoundError, version
from importlib.resources import Package, open_text
from pathlib import Path
from types import MappingProxyType

import attr
from cattrs import Converter

from .log import getLogger

LayoutType = ty.Literal["flat", "src"]
NameFormatType = ty.Literal["git", "docker", "hive"]

TIMESTAMP_FORMAT = "%Y%m%d%H%M%S"
CALGITVER_NO_SECONDS_FORMAT = "%Y%m%d.%H%M"

DOCKER_EXCLUSION_REGEX = r"[^\w\-\.]+"
DOCKER_SUB_CHARACTER = "-"
HIVE_EXCLUSION_REGEX = r"[\W]+"
HIVE_SUB_CHARACTER = "_"
VERSION_EXCLUSION_REGEX = r"[^\d\.]+"
VERSION_SUB_CHARACTER = ""

CI = "runner"
CI_TIMESTAMP = "CI_TIMESTAMP"
DEPLOYING = "DEPLOYING"
GIT_COMMIT = "GIT_COMMIT"
GIT_IS_CLEAN = "GIT_IS_CLEAN"
GIT_IS_DIRTY = "GIT_IS_DIRTY"
GIT_BRANCH = "GIT_BRANCH"
MAIN = "main"
THDS_USER = "THDS_USER"

META_FILE = "meta.json"

LOGGER = getLogger(__name__)


def format_name(name: str, format: NameFormatType = "git") -> str:
    if format == "git":
        return name
    elif format == "docker":
        return re.sub(DOCKER_EXCLUSION_REGEX, DOCKER_SUB_CHARACTER, name)
    elif format == "hive":
        return re.sub(HIVE_EXCLUSION_REGEX, HIVE_SUB_CHARACTER, name)
    else:
        raise ValueError(
            f"'{format}' is not a supported `format`. Supported formats: {ty.get_args(NameFormatType)}"
        )


@ty.overload
def get_timestamp() -> str:
    ...  # pragma: no cover


@ty.overload
def get_timestamp(as_datetime: ty.Literal[True]) -> datetime:
    ...  # pragma: no cover


@ty.overload
def get_timestamp(as_datetime: ty.Literal[False]) -> str:
    ...  # pragma: no cover


def get_timestamp(as_datetime=False):
    timestamp = datetime.now(timezone.utc)
    return timestamp.strftime(TIMESTAMP_FORMAT) if not as_datetime else timestamp


def make_calgitver() -> str:
    """Uses local git repo info to construct a more informative CalVer version string.

    This time format was chosen to be CalVer-esque but to drop time
    fractions smaller than minutes since they're exceeding rarely
    semantically meaningful, and the git commit hash will in 99.999%
    of cases be a great disambiguator for cases where multiple
    versions happen to be generated within the same minute by
    different users.

    We use only dots as separators to be compatible with both Container Registry
    formats and PEP440.
    """
    return "-".join(
        [
            datetime.now(tz=timezone.utc).strftime(CALGITVER_NO_SECONDS_FORMAT),
            get_commit()[:7],
            "" if is_clean() else "dirty",
        ]
    ).rstrip("-")


CALGITVER_EXTRACT_RE = re.compile(
    r"""
    (?P<year>\d{4})
    (?P<month>\d{2})
    (?P<day>\d{2})
    \.
    (?P<hour>\d{2})
    (?P<minute>\d{2})
    -
    (?P<git_commit>[a-f0-9]{7})
    (?P<dirty>(-dirty$)|$)
    """,
    re.X,
)


def parse_calgitver(maybe_calgitver: str):
    return CALGITVER_EXTRACT_RE.match(maybe_calgitver)


@ty.overload
def extract_timestamp(version: str) -> str:
    """Returns timestamp in full YYYYMMDDHHMMSS format even if the input was a CalGitVer string with no seconds."""


@ty.overload
def extract_timestamp(version: str, as_datetime: ty.Literal[True]) -> datetime:
    ...  # pragma: no cover


@ty.overload
def extract_timestamp(version: str, as_datetime: ty.Literal[False]) -> str:
    ...  # pragma: no cover


def extract_timestamp(version: str, as_datetime: bool = False):
    def to_result(dt: datetime):
        return dt.replace(tzinfo=timezone.utc) if as_datetime else dt.strftime(TIMESTAMP_FORMAT)

    # This is intended to be general-purpose and therefore a bit heuristic.
    # We attempt to parse the version as CalGitVer first, since it is a
    # narrow format. Failing that, we'll try SemCalVer.
    if parse_calgitver(version):
        try:
            return to_result(datetime.strptime(version[:13], CALGITVER_NO_SECONDS_FORMAT))
        except ValueError:
            pass

    version = re.sub(VERSION_EXCLUSION_REGEX, VERSION_SUB_CHARACTER, version)
    version_ = version.split(".")
    if len(version_) >= 3:
        try:
            return to_result(datetime.strptime(version_[2], TIMESTAMP_FORMAT))
        except ValueError:
            pass

    raise ValueError(
        f"`version`: {version} is not a timestamp-containing version string (SemCalVer or CalGitVer)."
    )


def _simple_run(s_or_l_cmd: ty.Union[str, ty.List[str]]) -> str:
    cmd = s_or_l_cmd.split() if isinstance(s_or_l_cmd, str) else s_or_l_cmd
    return sp.check_output(cmd, text=True).rstrip("\n")


def norm_name(pkg: str) -> str:
    """Apparently poetry creates slightly different dist-info
    directories and METADATA files than p-i-p-e-n-v did.
    """
    return pkg.replace(".", "_")


@lru_cache(None)
def get_version(pkg: Package) -> str:
    try:
        version_ = version(norm_name(str(pkg)))
    except PackageNotFoundError:
        try:
            version_ = version(str(pkg))
        except PackageNotFoundError:
            pkg_ = pkg.split(".")
            if len(pkg_) <= 1:
                return ""
            else:
                return get_version(".".join(pkg_[:-1]))

    return version_


def get_commit(pkg: Package = "") -> str:
    if GIT_COMMIT in os.environ:
        LOGGER.debug("`get_commit` reading from env var.")
        return os.environ[GIT_COMMIT]

    try:
        LOGGER.debug("`get_commit` reading from Git repo.")
        # backup in case you don't have `git` installed as a dev-dependency
        # but you still have the git repo available.
        return _simple_run("git rev-parse --verify HEAD")
    except (sp.CalledProcessError, FileNotFoundError):
        pass  # FileNotFoundError can happen if git is not installed at all.

    try:
        if pkg:
            LOGGER.debug("`get_commit` reading from metadata.")
            metadata = read_metadata(pkg)
            if metadata.is_empty:
                raise EmptyMetadataException
            return metadata.git_commit
    except EmptyMetadataException:
        pass

    LOGGER.debug("`get_commit` found no commit.")
    return ""


def is_clean(pkg: Package = "") -> bool:
    if GIT_IS_CLEAN in os.environ:
        LOGGER.debug("`is_clean` reading from env var.")
        return bool(os.environ[GIT_IS_CLEAN])

    if GIT_IS_DIRTY in os.environ:
        # compatibility with docker-tools/build_push
        return bool(os.getenv(GIT_IS_DIRTY))

    try:
        LOGGER.debug("`is_clean` reading from Git repo.")
        # command will print an empty string if the repo is clean
        return "" == _simple_run("git diff --name-status")
    except (sp.CalledProcessError, FileNotFoundError):
        pass  # FileNotFoundError can happen if git is not installed at all.

    try:
        if pkg:
            LOGGER.debug("`is_clean` reading from metadata.")
            metadata = read_metadata(pkg)
            if metadata.is_empty:
                raise EmptyMetadataException
            return bool(metadata.git_is_clean)
    except EmptyMetadataException:
        pass

    LOGGER.debug("`is_clean` found no cleanliness - assume dirty.")
    return True


def get_branch(pkg: Package = "", format: NameFormatType = "git") -> str:
    def _get_branch(pkg: Package = "") -> str:
        if GIT_BRANCH in os.environ:
            LOGGER.debug("`get_branch` reading from env var.")
            return os.environ[GIT_BRANCH]

        try:
            LOGGER.debug("`get_branch` reading from Git repo.")
            return _simple_run("git branch --show-current")
        except (sp.CalledProcessError, FileNotFoundError):
            pass  # FileNotFoundError can happen if git is not installed at all.

        try:
            if pkg:
                LOGGER.debug("`get_branch` reading from metadata.")
                metadata = read_metadata(pkg)
                if not metadata.git_branch:
                    raise EmptyMetadataException
                return metadata.git_branch
        except EmptyMetadataException:
            pass

        LOGGER.debug("`get_branch` found no branch.")
        return ""

    return format_name(_get_branch(pkg), format)


def get_user(pkg: Package = "", format: NameFormatType = "git") -> str:
    def _get_user(pkg: Package = "") -> str:
        if THDS_USER in os.environ:
            LOGGER.debug("`get_user` reading from env var.")
            return os.environ[THDS_USER]

        try:
            if pkg:
                LOGGER.debug("`get_user` reading from metadata.")
                metadata = read_metadata(pkg)
                if not metadata.thds_user:
                    raise EmptyMetadataException
                return metadata.thds_user
        except EmptyMetadataException:
            pass

        LOGGER.debug("`get_user` found no user data - getting system user.")
        return getuser()

    return format_name(_get_user(pkg), format)


def is_deployed(pkg: Package) -> bool:
    meta = read_metadata(pkg)
    if meta.is_empty:
        return False
    return True


MetaPrimitiveType = ty.Union[str, int, float, bool]
MiscType = ty.Mapping[str, MetaPrimitiveType]


@attr.frozen
class Metadata:
    git_commit: str = ""
    git_branch: str = ""
    git_is_clean: str = ""
    thds_user: str = ""
    misc: MiscType = attr.field(factory=lambda: MappingProxyType(dict()))

    @property
    def docker_branch(self) -> str:
        return format_name(self.git_branch, "docker")

    @property
    def hive_branch(self) -> str:
        return format_name(self.git_branch, "hive")

    @property
    def docker_user(self) -> str:
        return format_name(self.thds_user, "docker")

    @property
    def hive_user(self) -> str:
        return format_name(self.thds_user, "hive")

    @property
    def is_empty(self) -> bool:
        return all(not getattr(self, field.name) for field in attr.fields(Metadata))


meta_converter = Converter(forbid_extra_keys=True)
meta_converter.register_structure_hook(MiscType, lambda misc, _: MappingProxyType(misc))
# TODO - figure out typing issue for unstructure hook
meta_converter.register_unstructure_hook(MiscType, lambda misc: dict(misc))  # type: ignore


class EmptyMetadataException(Exception):
    pass


def init_metadata(misc: ty.Optional[ty.Mapping[str, MetaPrimitiveType]] = None) -> Metadata:
    clean = is_clean()

    return Metadata(
        git_commit=get_commit(),
        git_branch=get_branch(),
        git_is_clean="True" if clean else "",
        thds_user=get_user(),
        misc=MappingProxyType(misc) if misc else MappingProxyType(dict()),
    )


def write_metadata(
    pkg: str,
    *,
    misc: ty.Optional[ty.Mapping[str, MetaPrimitiveType]] = None,
    namespace: str = "thds",
    layout: LayoutType = "src",
    wdir: ty.Optional[Path] = None,
    deploying: bool = False,
) -> None:
    wdir = wdir or Path(".")
    assert wdir
    if os.getenv(DEPLOYING) or deploying:
        LOGGER.debug("Writing metadata.")
        metadata = init_metadata(misc=misc)
        metadata_path = os.path.join(
            "src" if layout == "src" else "",
            namespace,
            pkg.replace("-", "_").replace(".", "/"),
            META_FILE,
        )

        with open(wdir / metadata_path, "w") as f:
            LOGGER.info(f"Writing metadata for {pkg} to {wdir / metadata_path}")
            json.dump(meta_converter.unstructure(metadata), f, indent=2)
            f.write("\n")  # Add newline because Py JSON does not


@lru_cache(None)
def read_metadata(pkg: Package) -> Metadata:
    LOGGER.debug("Reading metadata.")

    if pkg == "__main__":
        raise ValueError("`read_meta` expects a package or module name, not '__main__'")

    if pkg is None:
        raise ValueError(
            "`read_meta` expects a package or module name, not `None`. "
            "If using `__package__` make sure an __init__.py is present."
        )

    try:
        with open_text(pkg, META_FILE) as f:
            metadata = meta_converter.structure(json.load(f), Metadata)
    # pkg=__name__ will raise a TypeError unless it is called in an __init__.py
    except (FileNotFoundError, TypeError):
        pkg_ = pkg.split(".")
        if len(pkg_) <= 1:
            metadata = Metadata()
        else:
            return read_metadata(".".join(pkg_[:-1]))

    return metadata
