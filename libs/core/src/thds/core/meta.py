import json
import os
import re
import typing as ty
from datetime import datetime, timezone
import dataclasses
from functools import lru_cache
from getpass import getuser
from importlib.metadata import PackageNotFoundError, version
from importlib.resources import Package, open_text
from types import MappingProxyType

from .log import getLogger

LayoutType = ty.Literal["flat", "src"]
NamespaceType = ty.Literal["thds", "thds/features"]
NameFormatType = ty.Literal["git", "docker", "hive"]

TIMESTAMP_FORMAT = "%Y%m%d%H%M%S"
CALVER_FORMAT = "%Y%m%d.%H%M%S"

DOCKER_EXCLUSION_REGEX = r"[^\w\-\.]+"
DOCKER_SUB_CHARACTER = "-"
HIVE_EXCLUSION_REGEX = r"[\W]+"
HIVE_SUB_CHARACTER = "_"

DEPLOYING = "DEPLOYING"
GIT_COMMIT = "GIT_COMMIT"
GIT_IS_DIRTY = "GIT_IS_DIRTY"
GIT_BRANCH = "GIT_BRANCH"
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


def get_calver() -> str:
    timestamp = datetime.now(timezone.utc)
    return timestamp.strftime(CALVER_FORMAT)


@ty.overload
def extract_timestamp(version: str) -> str:
    ...  # pragma: no cover


@ty.overload
def extract_timestamp(version: str, as_datetime: ty.Literal[True]) -> ty.Optional[datetime]:
    ...  # pragma: no cover


@ty.overload
def extract_timestamp(version: str, as_datetime: ty.Literal[False]) -> str:
    ...  # pragma: no cover


def extract_timestamp(version: str, as_datetime: bool = False):
    version_ = version.split(".")

    if len(version_) == 3:
        try:
            timestamp = datetime.strptime(version_[2], TIMESTAMP_FORMAT)
            return (
                timestamp.strftime(TIMESTAMP_FORMAT)
                if not as_datetime
                else timestamp.replace(tzinfo=timezone.utc)
            )
        except ValueError:
            return "" if not as_datetime else None
    elif len(version_) == 2:
        try:
            timestamp = datetime.strptime(version, CALVER_FORMAT)
            return (
                timestamp.strftime(TIMESTAMP_FORMAT)
                if not as_datetime
                else timestamp.replace(tzinfo=timezone.utc)
            )
        except ValueError:
            return "" if not as_datetime else None

    raise ValueError(f"`version`: {version} is not a valid version string (SemVer or CalVer).")


@lru_cache(None)
def get_version(pkg: Package) -> str:
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
        import git

        try:
            repo = git.Repo(search_parent_directories=True)
            LOGGER.debug("`get_commit` reading from Git repo.")
            return repo.head.object.hexsha[:7]
        except git.InvalidGitRepositoryError:
            pass
    except ImportError:  # pragma: no cover
        pass

    try:
        if pkg:
            LOGGER.debug("`get_commit` reading from metadata.")
            metadata = read_metadata(pkg)
            if not metadata.git_commit:
                raise EmptyMetadataException
            return metadata.git_commit
    except EmptyMetadataException:
        pass

    LOGGER.debug("`get_commit` found no commit.")
    return ""


def is_dirty(pkg: Package = "") -> bool:
    if GIT_IS_DIRTY in os.environ:
        LOGGER.debug("`is_dirty` reading from env var.")
        return bool(os.environ[GIT_IS_DIRTY])

    try:
        import git

        try:
            repo = git.Repo(search_parent_directories=True)
            LOGGER.debug("`is_dirty` reading from Git repo.")
            return repo.is_dirty()
        except git.InvalidGitRepositoryError:
            pass
    except ImportError:  # pragma: no cover
        pass

    if pkg:
        LOGGER.debug("`is_dirty` reading from metadata.")
        metadata = read_metadata(pkg)
        return bool(metadata.git_is_dirty)

    LOGGER.debug("`is_dirty` found no dirtiness - assume dirty.")
    return True


def get_branch(pkg: Package = "", format: NameFormatType = "git") -> str:
    def _get_branch(pkg: Package = "") -> str:
        if GIT_BRANCH in os.environ:
            LOGGER.debug("`get_branch` reading from env var.")
            return os.environ[GIT_BRANCH]

        try:
            import git

            try:
                repo = git.Repo(search_parent_directories=True)
                LOGGER.debug("`get_branch` reading from Git repo.")
                return repo.active_branch.name
            except git.InvalidGitRepositoryError:
                pass
        except ImportError:  # pragma: no cover
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


MetaPrimitiveType = ty.Union[str, int, float, bool]
MiscType = ty.Mapping[str, MetaPrimitiveType]


@dataclasses.dataclass(frozen=True)
class Metadata:
    git_commit: str = ""
    git_branch: str = ""
    git_is_dirty: str = ""
    thds_user: str = ""
    misc: MiscType = dataclasses.field(default_factory=lambda: MappingProxyType(dict()))

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


def structure_metadata(payload: ty.Mapping[str, ty.Union[MetaPrimitiveType, MiscType]]) -> Metadata:
    payload_fields = set(payload.keys())
    expected_fields = set((field.name for field in dataclasses.fields(Metadata)))

    if payload_fields != expected_fields:
        raise BadMetadataException(
            f"Project metadata expects these exact fields: {expected_fields}, got : {payload_fields}"
        )

    return Metadata(
        git_commit=payload["git_commit"],
        git_branch=payload["git_branch"],
        thds_user=payload["thds_user"],
        misc=MappingProxyType(payload["misc"]),
    )


def unstructure_metadata(
    metadata: Metadata,
) -> ty.Dict[str, ty.Union[str, ty.Dict[MetaPrimitiveType, MetaPrimitiveType]]]:
    payload_fields = set((field.name for field in dataclasses.fields(Metadata)))
    payload = {
        field: getattr(metadata, field) if field != "misc" else dict(getattr(metadata, field))
        for field in payload_fields
    }

    return payload


class EmptyMetadataException(Exception):
    pass


class BadMetadataException(Exception):
    pass


def init_metadata(misc: ty.Optional[ty.Mapping[str, MetaPrimitiveType]] = None) -> Metadata:
    dirty = is_dirty()

    return Metadata(
        git_commit=get_commit(),
        git_branch=get_branch(),
        git_is_dirty="True" if dirty else "",
        thds_user=get_user(),
        misc=MappingProxyType(misc) if misc else MappingProxyType(dict()),
    )


def write_metadata(
    pkg: str,
    *,
    misc: ty.Optional[ty.Mapping[str, MetaPrimitiveType]] = None,
    namespace: NamespaceType = "thds",
    layout: LayoutType = "src",
) -> None:
    if os.getenv(DEPLOYING):
        LOGGER.debug("Writing metadata.")
        metadata = init_metadata(misc=misc)

        metadata_path = os.path.join("src" if layout == "src" else "", namespace, pkg, META_FILE)

        with open(metadata_path, "w") as f:
            json.dump(unstructure_metadata(metadata), f)
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
            metadata = structure_metadata(json.load(f))
    # pkg=__name__ will raise a TypeError unless it is called in an __init__.py
    except (FileNotFoundError, TypeError):
        pkg_ = pkg.split(".")
        if len(pkg_) <= 1:
            metadata = Metadata()
        else:
            return read_metadata(".".join(pkg_[:-1]))

    return metadata
