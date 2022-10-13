import os
import shutil
import subprocess

from ..common.decorators import clean_metadata, export_metadata, resolve_pyproject
from ..common.util import path_from_repo
from ...log import getLogger
from ...types import StrOrPath


LOGGER = getLogger(__name__)

ARTIFACTORY_REPOSITORY = "ds-pypi-releases-local"
VERSION_PATTERN = r"[0-9]+(?:\.[0-9]+){1,2}"


def _build(path: StrOrPath) -> None:
    LOGGER.info("Building package at %s", path_from_repo(path))

    dist_path = os.path.join(path, "dist")
    if os.path.isdir(dist_path):
        LOGGER.info(
            "'dist' folder already exists at: '%s'. Removing before rebuilding the distribution.",
            path_from_repo(dist_path),
        )
        shutil.rmtree(dist_path)
    build_cmd = ["python", "-m", "build", path]
    build_status = subprocess.run(build_cmd)
    if build_status.returncode != 0:
        raise subprocess.CalledProcessError(
            returncode=build_status.returncode,
            cmd=" ".join(build_cmd),
            output="Could not build package, please see output above to debug.",
        )


def build(path: StrOrPath) -> None:
    clean_metadata(path=path)(export_metadata(resolve_pyproject(path=path)(_build)))(path=path)


def release(path: StrOrPath, skip_build: bool = False) -> None:
    if not skip_build:
        build(path)

    LOGGER.info("Releasing package at %s", path_from_repo(path))

    release_cmd = [
        "jfrog",
        "rt",
        "u",
        "--regexp",
        os.path.join(path, rf"dist/([a-zA-Z][\.\w]+)-({VERSION_PATTERN})-py3.*\.whl"),
        f"{ARTIFACTORY_REPOSITORY}" + "/{1}/{2}/",
    ]
    release_status = subprocess.run(release_cmd)
    if release_status.returncode != 0:
        raise subprocess.CalledProcessError(
            returncode=release_status.returncode,
            cmd=" ".join(release_cmd),
            output="Could not release to Artifactory, please see output above to debug.",
        )
