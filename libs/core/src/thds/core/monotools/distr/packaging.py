# This is heavily inspired by Dan Hipschman of OpenDoor's gist here:
# https://gist.github.com/dan-hipschman-od/070318806610727f17b2d6616ceaa4cc
# The above is referenced in the Q&A from Hipschman's great article here:
# https://medium.com/opendoor-labs/our-python-monorepo-d34028f2b6fa
import argparse
import functools
import os
import shutil
import subprocess
import typing as ty
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

from ..util import load_pyproject, load_pipfile_dependencies
from ... import __version__
from ...log import getLogger
from ...meta import DEPLOYING, META_FILE, init_metadata, unstructure_metadata

try:
    import build  # noqa: F401
    import toml
except ImportError:  # pragma: no cover
    raise RuntimeError("Install `build` and `toml` (`core[dev]`) to run the `release` CLI.")

LOGGER = getLogger(__name__)

PYPROJECT_FILE = "pyproject.toml"
STASHED_PYRPOJECT_FILE = "pyproject.toml.stashed"
PIPFILE = "Pipfile"
ARTIFACTORY_REPOSITORY = "ds-pypi-releases-local"
VERSION_PATTERN = r"[0-9]+(?:\.[0-9]+){1,2}"


@contextmanager
def stash_file(filename: str, stash_name: str) -> ty.Iterator[None]:
    """In the context of `with stash_file("a", "b"): ...`, the file named "a" will be renamed
    to "b". Upon leaving the context, the file named "a" will be restored to its original
    contents, and file "b" will be deleted. An exception is raised if "b" already exists."""

    try:
        os.rename(filename, stash_name)
    except FileNotFoundError as e:
        raise EnvironmentError(f"No such file: {filename}") from e
    except OSError as e:
        raise EnvironmentError(f"Please remove {stash_name}") from e

    try:
        yield
    finally:
        os.replace(stash_name, filename)


def clean_metadata(func):
    def _clean_metadata() -> None:
        LOGGER.info("Cleaning local metadata file(s).")
        meta_files = Path().cwd().rglob(META_FILE)
        for path in meta_files:
            if path.is_file():
                path.unlink()

    @functools.wraps(func)
    def wrapper_clean_metadata(*args, **kwargs):
        try:
            retval = func(*args, **kwargs)
        except Exception:
            _clean_metadata()
            raise
        _clean_metadata()
        return retval

    return wrapper_clean_metadata


def export_metadata(func):
    @functools.wraps(func)
    def wrapper_export_metadata(*args, **kwargs):
        os.environ[DEPLOYING] = "1"
        metadata = unstructure_metadata(init_metadata())
        del metadata["misc"]
        LOGGER.info("Exporting metadata:\n" "%s", metadata)
        for k, v in metadata.items():
            if k != "misc":
                os.environ[k.upper()] = str(v)
        return func(*args, **kwargs)

    return wrapper_export_metadata


def _determine_release_version(pyproject_data: ty.Dict, incl_patch: bool) -> str:
    version_str = pyproject_data["project"]["version"]

    if version_str.count(".") != 1:
        raise ValueError("Version must be major.minor, patch will be added.")

    patch_version = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S") if incl_patch else ""

    return f"{version_str}.{patch_version}" if patch_version else version_str


def _resolve_local_deps(dependencies: ty.Dict, package_name: str) -> ty.List[str]:
    dependencies_to_add = []

    for dep_name, dep_value in dependencies.items():
        if isinstance(dep_value, dict) and "path" in dep_value and dep_name != package_name:
            # Found a non-self, local dependency. Load it's pyproject.toml to get its version
            dep_pyproject_file = f"{dep_value['path']}/{PYPROJECT_FILE}"
            dep_pyproject_data = toml.load(dep_pyproject_file)
            dep_version = dep_pyproject_data["project"]["version"]
            if dep_version.count(".") != 1:
                raise ValueError(f"Version in {dep_pyproject_file} must be major.minor")

            dep_version_parts = tuple(int(v) for v in dep_version.split("."))
            next_minor_version = ".".join([str(dep_version_parts[0]), str(dep_version_parts[1] + 1)])

            # The requirement should be to use the most recent published version, but
            # add a max version constraint so we don't pull in future versions that might
            # accidentally break things.
            dep_version_requirement = f">={dep_version},<{next_minor_version}"
            dependencies_to_add.append(f"{dep_name}{dep_version_requirement}")

    return dependencies_to_add


# TODO - refactor this into smaller functions for readability
def resolve_deps(incl_patch: bool = True):
    try:
        import toml
    except ImportError:  # pragma: no cover
        raise RuntimeError("The `toml` package is needed, install `core[dev]`.")

    def decorator_resolve_deps(func):
        @functools.wraps(func)
        def wrapper_resolve_deps(*args, **kwargs):
            with stash_file(PYPROJECT_FILE, STASHED_PYRPOJECT_FILE):
                package_name, pyproject_data = load_pyproject(STASHED_PYRPOJECT_FILE)

                # Add a timestamp for the patch version
                pyproject_data["project"]["version"] = _determine_release_version(
                    pyproject_data, incl_patch
                )

                # Replace local dependencies with published dependencies
                dependencies = load_pipfile_dependencies(PIPFILE)
                dependencies_to_add = _resolve_local_deps(dependencies, package_name)

                # Append published verions of local dependencies to pyproject.toml's dependencies
                try:
                    pyproject_data["project"]["dependencies"].extend(dependencies_to_add)
                except KeyError:
                    pass

                with open(PYPROJECT_FILE, "w") as output_file:
                    toml.dump(pyproject_data, output_file)

                return func(*args, **kwargs)

        return wrapper_resolve_deps

    return decorator_resolve_deps


# TODO - how to make calling `resolve_deps` decorator optional?
@clean_metadata
@export_metadata
@resolve_deps()
def _build() -> None:
    if os.path.isdir("dist"):
        LOGGER.info("'dist' folder already exists. Removing before rebuilding the distribution.")
        shutil.rmtree("dist")
    build_cmd = ["python", "-m", "build", "."]
    build_status = subprocess.run(build_cmd)
    if build_status.returncode != 0:
        raise subprocess.CalledProcessError(
            returncode=build_status.returncode,
            cmd=" ".join(build_cmd),
            output="Could not build package, please see output above to debug.",
        )


def _release() -> None:
    release_cmd = [
        "jfrog",
        "rt",
        "u",
        "--regexp",
        rf"dist/([a-zA-Z][\.\w]+)-({VERSION_PATTERN})-py3.*\.whl",
        f"{ARTIFACTORY_REPOSITORY}" + "/{1}/{2}/",
    ]
    release_status = subprocess.run(release_cmd)
    if release_status.returncode != 0:
        raise subprocess.CalledProcessError(
            returncode=release_status.returncode,
            cmd=" ".join(release_cmd),
            output="Could not release to Artifactory, please see output above to debug.",
        )


def release():
    """
    Assumes there's a pyproject.toml file in the current working directory. This function
    adds a patch version, replaces local dependencies with published ones, and releases a
    wheel file to Artifactory.

    Note: this does not currently include dev-dependencies.
    """
    parser = argparse.ArgumentParser(description="release package to Artifactory")
    parser.add_argument("-v", "--version", action="version", version=f"{__version__}")
    _ = parser.parse_args()

    _build()
