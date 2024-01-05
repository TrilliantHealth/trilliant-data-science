import json
import os
import re
import typing as ty
from datetime import datetime, timezone
from functools import lru_cache
from getpass import getuser
from importlib.metadata import PackageNotFoundError, version
from importlib.resources import Package, open_text
from pathlib import Path
from types import MappingProxyType

import attrs
from cattrs import Converter

from . import git
from .log import getLogger
from .types import StrOrPath

LayoutType = ty.Literal["flat", "src"]
NameFormatType = ty.Literal["git", "docker", "hive"]

TIMESTAMP_FORMAT = "%Y%m%d%H%M%S"

DOCKER_EXCLUSION_REGEX = r"[^\w\-.]+"
DOCKER_SUB_CHARACTER = "-"
HIVE_EXCLUSION_REGEX = r"[\W]+"
HIVE_SUB_CHARACTER = "_"
VERSION_EXCLUSION_REGEX = r"[^\d.]+"
VERSION_SUB_CHARACTER = ""

CI_TIMESTAMP = "CI_TIMESTAMP"
CI_USER = "runner"
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


def get_timestamp(as_datetime: bool = False):
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
    dirty = "" if is_clean() else "dirty"
    base_components: ty.Tuple[str, ...] = tuple()
    if not dirty:
        # we only attempt this 'determinstic' datetime if the repo is clean, because if
        # it's not clean then this isn't deterministic anyway, and so we'd rather just
        # have an up-to-date timestamp
        try:
            commit_datetime, commit_hash = git.get_commit_datetime_and_hash()
            base_components = (commit_datetime, commit_hash[:7])
        except git.NO_GIT:
            pass
    if not base_components:
        base_components = (
            datetime.now(tz=timezone.utc).strftime(git.CALGITVER_NO_SECONDS_FORMAT),
            get_commit()[:7],
        )

    return "-".join((*base_components, dirty)).rstrip("-")


def print_calgitver():
    print(make_calgitver())


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
            return to_result(datetime.strptime(version[:13], git.CALGITVER_NO_SECONDS_FORMAT))
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


def norm_name(pkg: str) -> str:
    """Apparently poetry creates slightly different dist-info
    directories and METADATA files than p-i-p-e-n-v did.
    """
    return pkg.replace(".", "_")


@lru_cache(None)
def get_version(pkg: Package, orig: str = "") -> str:
    try:
        version_ = version(norm_name(str(pkg)))
    except PackageNotFoundError:
        try:
            version_ = version(str(pkg))
        except PackageNotFoundError:
            # 'recurse' upward, assuming that the package name is overly-specified
            pkg_ = pkg.split(".")
            if len(pkg_) <= 1:
                # we're just about to give up. Before we do, check to see if there's a
                # meta.json file hanging around, and if so, see if it contains a pyproject_version.
                metadata = read_metadata(orig or pkg)
                if metadata and metadata.pyproject_version:
                    return metadata.pyproject_version
                LOGGER.warning("Could not find a version for `%s`. Package not found.", orig or pkg)
                return ""
            return get_version(".".join(pkg_[:-1]), orig or pkg)

    return version_


@lru_cache(None)
def get_base_package(pkg: Package) -> str:
    try:
        _ = version(norm_name(str(pkg)))
    except PackageNotFoundError:
        try:
            _ = version(str(pkg))
        except PackageNotFoundError:
            pkg_ = pkg.split(".")
            if len(pkg_) <= 1:
                LOGGER.warning("Could not find the base package for `%s`. Package not found.", pkg)
                return ""
            else:
                return get_base_package(".".join(pkg_[:-1]))

    return str(pkg)


def get_repo_name() -> str:
    try:
        return git.get_repo_name()
    except git.NO_GIT:
        LOGGER.debug("`get_repo_name` found no repo name.")
        return ""


def get_commit(pkg: Package = "") -> str:  # should really be named get_commit_hash
    if GIT_COMMIT in os.environ:
        LOGGER.debug("`get_commit` reading from env var.")
        return os.environ[GIT_COMMIT]

    try:
        return git.get_commit_hash()
    except git.NO_GIT:
        pass

    try:
        if pkg:
            LOGGER.debug("`get_commit` reading from metadata.")
            metadata = read_metadata(pkg)
            if metadata.is_empty:
                raise EmptyMetadataException
            return metadata.git_commit
    except EmptyMetadataException:
        pass

    LOGGER.warning("`get_commit` found no commit.")
    return ""


def is_clean(pkg: Package = "") -> bool:
    if GIT_IS_CLEAN in os.environ:
        LOGGER.debug("`is_clean` reading from env var.")
        return bool(os.environ[GIT_IS_CLEAN])

    if GIT_IS_DIRTY in os.environ:
        # compatibility with docker-tools/build_push
        LOGGER.debug("`is_clean` reading from env var.")
        return not bool(os.getenv(GIT_IS_DIRTY))

    try:
        return git.is_clean()
    except git.NO_GIT:
        pass

    try:
        if pkg:
            LOGGER.debug("`is_clean` reading from metadata.")
            metadata = read_metadata(pkg)
            if metadata.is_empty:
                raise EmptyMetadataException
            return metadata.git_is_clean
    except EmptyMetadataException:
        pass

    LOGGER.warning("`is_clean` found no cleanliness - assume dirty.")
    return False


def get_branch(pkg: Package = "", format: NameFormatType = "git") -> str:
    def _get_branch(pkg: Package = "") -> str:
        if GIT_BRANCH in os.environ:
            LOGGER.debug("`get_branch` reading from env var.")
            return os.environ[GIT_BRANCH]

        try:
            return git.get_branch()
        except git.NO_GIT:
            pass

        try:
            if pkg:
                LOGGER.debug("`get_branch` reading from metadata.")
                metadata = read_metadata(pkg)
                if not metadata.git_branch:
                    raise EmptyMetadataException
                return metadata.git_branch
        except EmptyMetadataException:
            pass

        LOGGER.warning("`get_branch` found no branch.")
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
    return not meta.is_empty


def _hacky_get_pyproject_toml_version(pkg: Package, wdir: Path) -> str:
    ppt = wdir / "pyproject.toml"
    if ppt.exists():
        with open(ppt) as f:
            toml = f.read()
        # it will be a good day when Python packages a toml reader by default.
        for line in toml.splitlines():
            if m := re.match(r"version\s*=\s*[\"'](?P<version>[a-zA-Z0-9.]+)[\"']", line):
                return m.group("version")
    return ""


MiscType = ty.Mapping[str, ty.Union[str, int, float, bool]]


@attrs.frozen
class Metadata:
    git_commit: str = ""
    git_branch: str = ""
    git_is_clean: bool = False
    pyproject_version: str = ""  # only present if the project defines `version` inside pyproject.toml
    thds_user: str = ""
    misc: MiscType = attrs.field(factory=lambda: MappingProxyType(dict()))

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
        return all(not getattr(self, field.name) for field in attrs.fields(Metadata))

    @property
    def git_is_dirty(self) -> bool:
        return not self.git_is_clean


meta_converter = Converter(forbid_extra_keys=True)
meta_converter.register_structure_hook(
    Metadata, lambda v, _: Metadata(misc=MappingProxyType(v.pop("misc", {})), **v)
)


class EmptyMetadataException(Exception):
    pass


def init_metadata(misc: ty.Optional[MiscType] = None, pyproject_toml_version: str = "") -> Metadata:
    return Metadata(
        git_commit=get_commit(),
        git_branch=get_branch(),
        git_is_clean=is_clean(),
        pyproject_version=pyproject_toml_version,
        thds_user=os.getenv(THDS_USER, getuser()),
        misc=MappingProxyType(misc) if misc else MappingProxyType(dict()),
    )


def _sanitize_metadata_for_docker_tools(d: dict):
    """We want our Docker builds to be able to take advantage of
    caching based on the contents of the sources copied over into
    them.  If we embed a meta.json into each library where the commit
    hash changes every time a commit happens, then we've blown away
    our entire cache.

    The Docker builds already inject this metadata as environment
    variables after the source copies happen, so there's no need for
    us to embed it this way.
    """
    d["git_commit"] = ""
    d["git_branch"] = ""
    d["git_is_clean"] = ""
    d["thds_user"] = THDS_USER


def write_metadata(
    pkg: str,
    *,
    misc: ty.Optional[MiscType] = None,
    namespace: str = "thds",
    layout: LayoutType = "src",
    wdir: ty.Optional[StrOrPath] = None,
    deploying: bool = False,
    for_docker_tools_build: bool = False,
) -> None:
    wdir_ = Path(wdir) if wdir else Path(".")
    assert wdir_
    if os.getenv(DEPLOYING) or deploying:
        LOGGER.debug("Writing metadata.")
        metadata = init_metadata(
            misc=misc, pyproject_toml_version=_hacky_get_pyproject_toml_version(pkg, wdir_)
        )
        metadata_path = os.path.join(
            "src" if layout == "src" else "",
            namespace.replace("-", "/").replace(".", "/"),
            pkg.replace("-", "_").replace(".", "/"),
            META_FILE,
        )

        LOGGER.info(f"Writing metadata for {pkg} to {wdir_ / metadata_path}")
        with open(wdir_ / metadata_path, "w") as f:
            metadata_dict = meta_converter.unstructure(metadata)
            if for_docker_tools_build:
                _sanitize_metadata_for_docker_tools(metadata_dict)
            json.dump(metadata_dict, f, indent=2)
            f.write("\n")  # Add newline because Py JSON does not


@lru_cache(None)
def read_metadata(pkg: Package) -> Metadata:
    LOGGER.debug("Reading metadata.")

    if pkg == "__main__":
        raise ValueError("`read_meta` expects a package or module name, not '__main__'.")

    if not pkg:
        raise ValueError(
            "`read_meta` is missing a package or module name. "
            "If using `__package__` make sure an __init__.py is present."
        )

    try:
        with open_text(pkg, META_FILE) as f:
            return meta_converter.structure(json.load(f), Metadata)
    # pkg=__name__ will raise a TypeError unless it is called in an __init__.py
    except (ModuleNotFoundError, FileNotFoundError, TypeError):
        pkg_ = pkg.split(".")
        if len(pkg_) <= 1:
            return Metadata()
        return read_metadata(".".join(pkg_[:-1]))
