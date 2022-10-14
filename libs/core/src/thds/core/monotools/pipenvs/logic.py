import os
import subprocess
from concurrent.futures import ProcessPoolExecutor

from ...log import getLogger
from ..common.constants import DAG_ROOT
from ..common.datamodels import ProjectSpec
from ..common.util import find_repo_root, in_directory
from ..dag import build_repo_dag

LOGGER = getLogger(__name__)


def _sync():
    cmd = ["pipenv", "sync", "--dev"]
    status = subprocess.run(cmd)

    if status.returncode != 0:
        raise subprocess.CalledProcessError(
            returncode=status.returncode,
            cmd=" ".join(cmd),
            output="Pipenv could not sync dependencies, please see output above to debug.",
        )


def _sync_project(project: ProjectSpec):
    with in_directory(project.path):
        LOGGER.info("Syncing dependencies for '%s' (in directory '%s')...", project.name, project.path)
        _sync()


def sync(project: str = "", serial: bool = False, load_dag: bool = False):
    # TODO - sync projects instead of just one project + deps or everything.
    #   Would we ever need a sync without dev deps?
    os.environ["PIPENV_IGNORE_VIRTUALENVS"] = "1"

    with in_directory(find_repo_root()):
        dag = build_repo_dag(load=load_dag)
        build_order = dag.determine_build_order()
        deps = dag.find_shortest_path(DAG_ROOT, project) if project else []

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
