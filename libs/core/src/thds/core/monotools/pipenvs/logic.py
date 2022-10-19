import json
import os
import subprocess
import typing as ty
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
from functools import partial
from pathlib import Path

import attr
import cattr

try:
    import toml
except ModuleNotFoundError:
    raise ModuleNotFoundError(
        f"'toml' must be installed ('thds.core[dev]') to use code from '{__name__}'."
    )

from ...log import getLogger
from ..common.constants import PIPFILE, PROJECT_CONFIG, PYPROJECT_FILE
from ..common.datamodels import ProjectSpec
from ..common.util import find_repo_root, in_directory, md5_file
from ..dag import build_repo_dag

LOGGER = getLogger(__name__)


DEPENDENCIES_LOCK = "dependencies.lock"
PIPFILE_LOCK = "Pipfile.lock"


@attr.frozen
class DependenciesLock:
    pipfile: str
    pipfile_lock: str
    runtime_deps: ty.Set[str] = attr.field(factory=set)
    dev_deps: ty.Set[str] = attr.field(factory=set)


converter = cattr.Converter()
converter.register_structure_hook(ty.Set[str], lambda s, _: set(s))
converter.register_unstructure_hook(ty.Set[str], lambda s: list(s))


def _sync() -> None:
    cmd = ["pipenv", "sync", "--dev"]
    status = subprocess.run(cmd)

    if status.returncode != 0:
        raise subprocess.CalledProcessError(
            returncode=status.returncode,
            cmd=" ".join(cmd),
            output="Pipenv could not sync dependencies, please see output above to debug.",
        )


def _sync_project(project: ProjectSpec) -> None:
    with in_directory(project.path):
        LOGGER.info("Syncing dependencies for '%s' (in directory '%s')...", project.name, project.path)
        _sync()


def sync(project: str = "", serial: bool = False, load_dag: bool = False) -> None:
    # TODO - sync projects instead of just one project + deps or everything.
    #   Would we ever need a sync without dev deps?
    os.environ["PIPENV_IGNORE_VIRTUALENVS"] = "1"

    with in_directory(find_repo_root()):
        dag = build_repo_dag(load=load_dag)
        build_order = dag.determine_build_order()
        deps = [project, *dag.get_ancestors(project)] if project else []

        for step in build_order:
            step_ = [s for s in step if s.name in deps] if project else step
            if not serial:
                with ProcessPoolExecutor() as executor:
                    executor.map(_sync_project, step_)
            else:
                for proj in step:
                    _sync_project(proj)

        LOGGER.info("Syncing repo root dependencies...")
        _sync()


def _ensure_project_conf(path: Path) -> Path:
    project_conf = Path(path, PROJECT_CONFIG)

    if project_conf.exists() and not project_conf.is_dir():
        raise FileExistsError(f"{project_conf} exists and is not a directory.")

    project_conf.mkdir(exist_ok=True)

    return project_conf


def _generate_lockfile(path: Path) -> DependenciesLock:
    pipfile_md5 = md5_file(path / PIPFILE)
    pipfile_lock_md5 = md5_file(path / PIPFILE_LOCK)
    pyproject_data = toml.load(path / PYPROJECT_FILE)
    runtime_deps_md5 = set(pyproject_data["project"].get("dependencies", []))
    dev_deps_md5 = set(pyproject_data["project"].get("optional-dependencies", {}).get("dev", []))

    return DependenciesLock(
        pipfile=pipfile_md5,
        pipfile_lock=pipfile_lock_md5,
        runtime_deps=runtime_deps_md5,
        dev_deps=dev_deps_md5,
    )


def _check_lockfile(path: Path) -> bool:
    if (
        not path.joinpath(PIPFILE_LOCK).exists()
        or not path.joinpath(PROJECT_CONFIG, DEPENDENCIES_LOCK).exists()
    ):
        return False

    conf_path = _ensure_project_conf(path)
    with open(path.joinpath(conf_path, DEPENDENCIES_LOCK), "r") as f:
        existing_lock = converter.structure(json.load(f), DependenciesLock)

    current_lock = _generate_lockfile(path)

    return current_lock == existing_lock


def _path_relative_from(path: Path) -> Path:
    steps = [s for s in str(path).split("/") if s != "."]
    return Path("/".join(".." for _ in steps))


def _lock() -> None:
    cmd = ["pipenv", "lock"]
    status = subprocess.run(cmd)

    if status.returncode != 0:
        raise subprocess.CalledProcessError(
            returncode=status.returncode,
            cmd=" ".join(cmd),
            output="Pipenv could not lock dependencies, please see output above to debug.",
        )


def _lock_wrapper(project: ProjectSpec, hashes_only: bool) -> None:
    if not hashes_only:
        LOGGER.info("Locking dependencies for '%s' (in directory '%s')...", project.name, project.path)
        _lock()

    dep_lock = _generate_lockfile(Path())
    LOGGER.info("Writing new dependency lock for '%s' (in directory '%s').", project.name, project.path)
    conf_path = _ensure_project_conf(Path())
    with open(Path(conf_path, DEPENDENCIES_LOCK), "w") as f:
        json.dump(converter.unstructure(dep_lock), f)
        f.write("\n")  # Add newline because Py JSON does not


def _lock_project(
    project: ProjectSpec,
    check: bool,
    force: bool,
    hashes_only: bool,
) -> ty.Dict[str, ty.Dict[str, ty.Union[bool, ty.Dict[str, bool]]]]:
    with in_directory(project.path):
        lock_statuses = defaultdict(dict)
        lock_statuses[project.name]["self"] = _check_lockfile(Path())
        lock_statuses[project.name]["deps"] = {
            dep.name: _check_lockfile(Path(_path_relative_from(project.path), dep.path))
            for dep in project.deps.all
        }

        if force:
            _lock_wrapper(project, hashes_only=hashes_only)
        elif check and (
            not lock_statuses[project.name]["self"]
            or any(not status for status in lock_statuses[project.name]["deps"].values())
        ):
            return lock_statuses
        elif not lock_statuses[project.name]["self"] or any(
            not status for status in lock_statuses[project.name]["deps"].values()
        ):
            _lock_wrapper(project, hashes_only=hashes_only)
        else:
            LOGGER.info(
                "Dependency lock up to date for '%s' (in directory '%s').", project.name, project.path
            )

        return dict()


def lock(
    project: str = "",
    check: bool = False,
    force: bool = False,
    hashes_only: bool = False,
    serial: bool = False,
    load_dag: bool = False,
) -> None:
    # TODO - sync projects instead of just one project + deps or everything.
    #   Would we ever need a sync without dev deps?
    os.environ["PIPENV_IGNORE_VIRTUALENVS"] = "1"
    lock_statuses = []

    if check and force:
        LOGGER.warning("Both `check` and `force` are set to `True` but `force` takes precendence.")

    if check and hashes_only:
        LOGGER.warning("Both `check` and `hashes_only` are set to `True` but `check` takes precendence.")

    with in_directory(find_repo_root()):
        dag = build_repo_dag(load=load_dag)
        build_order = dag.determine_build_order()
        deps = [project, *dag.get_ancestors(project)] if project else []

        for step in build_order:
            step_ = [s for s in step if s.name in deps] if project else step
            if not serial:
                partial_lock_project = partial(
                    _lock_project, check=check, force=force, hashes_only=hashes_only
                )
                with ProcessPoolExecutor() as executor:
                    lock_statuses.extend(executor.map(partial_lock_project, step_))
            else:
                for proj in step_:
                    lock_statuses.append(
                        _lock_project(proj, check=check, force=force, hashes_only=hashes_only)
                    )
        lock_statuses = list(filter(None, lock_statuses))
        if check and lock_statuses:
            raise ValueError(
                "Dependency lock(s) for the following project(s) and/or their dependencies are not "
                f"up to date (`False` indicates out-of-date): {lock_statuses}."
            )
