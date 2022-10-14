import os
import shutil
import subprocess

try:
    import build as build_
except ModuleNotFoundError:
    raise ModuleNotFoundError(
        f"'build' must be installed ('thds.core[dev]') to use code from '{__name__}'."
    )

from ...log import getLogger
from ...types import StrOrPath
from ..common.decorators import clean_metadata, export_metadata, resolve_pyproject

LOGGER = getLogger(__name__)

ARTIFACTORY_REPOSITORY = "ds-pypi-releases-local"
VERSION_PATTERN = r"[0-9]+(?:\.[0-9]+){1,2}"


def _build(path: StrOrPath) -> None:
    LOGGER.info("Building package at %s", path)

    dist_path = os.path.join(path, "dist")
    if os.path.isdir(dist_path):
        LOGGER.info(
            "'dist' folder already exists at: '%s'. Removing before rebuilding the distribution.",
            dist_path,
        )
        shutil.rmtree(dist_path)
    cmd = ["python", "-m", "build", path]
    status = subprocess.run(cmd)
    if status.returncode != 0:
        raise subprocess.CalledProcessError(
            returncode=status.returncode,
            cmd=" ".join(cmd),
            output="Could not build package, please see output above to debug.",
        )


def build(path: StrOrPath) -> None:
    clean_metadata(path=path)(export_metadata(resolve_pyproject(path=path)(_build)))(path=path)


def release(path: StrOrPath, skip_build: bool = False) -> None:
    if not skip_build:
        build(path)

    LOGGER.info("Releasing package at %s", path)

    cmd = [
        "jfrog",
        "rt",
        "u",
        "--regexp",
        os.path.join(path, rf"dist/([a-zA-Z][\.\w]+)-({VERSION_PATTERN})-py3.*\.whl"),
        f"{ARTIFACTORY_REPOSITORY}" + "/{1}/{2}/",
    ]
    status = subprocess.run(cmd)
    if status.returncode != 0:
        raise subprocess.CalledProcessError(
            returncode=status.returncode,
            cmd=" ".join(cmd),
            output="Could not release to Artifactory, please see output above to debug.",
        )
