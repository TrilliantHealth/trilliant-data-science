import itertools
import json
import os
import typing as ty
from pathlib import Path

import networkx as nx

from .common.constants import DAG_ROOT, PYPROJECT_FILE, REPO_CONFIG
from .common.datamodels import ProjectSpec, RepoDAG, converter
from .common.util import find_repo_root, git_changes, in_directory

SourceType = ty.Literal["apps", "libs"]

DAG_FILE = "dag.json"


class DAGException(Exception):
    pass


def _collect_projects() -> ty.Dict[str, ProjectSpec]:
    project_specs = {}

    for source in ty.get_args(SourceType):
        for path in Path(source).iterdir():
            if path.is_dir():
                project_spec = ProjectSpec.from_pyproject_file(path / PYPROJECT_FILE, incl_extras=False)
                project_specs[project_spec.name] = project_spec

    return project_specs


def _assert_no_app_deps(projects: ty.Iterable[ProjectSpec]) -> bool:
    dep_sources = itertools.chain.from_iterable(
        ((os.path.dirname(dep.path) for dep in project.deps.all) for project in projects)
    )
    return not any(source == "apps" for source in dep_sources)


def _ensure_repo_conf() -> Path:
    repo_conf = Path(REPO_CONFIG)

    if repo_conf.exists() and not repo_conf.is_dir():
        raise FileExistsError(f"{REPO_CONFIG} exists and is not a directory.")

    repo_conf.mkdir(exist_ok=True)

    return repo_conf


def build_repo_dag(save: bool = False, load: bool = False) -> RepoDAG:
    with in_directory(find_repo_root()):
        repo_conf = _ensure_repo_conf()
        dag_file = repo_conf / DAG_FILE

        if load:
            with open(dag_file, "r") as f:
                return converter.structure(json.load(f), RepoDAG)

        project_specs = _collect_projects()
        changes = git_changes()

        graph = nx.DiGraph()
        graph.add_nodes_from((spec.name for spec in project_specs.values()))
        graph.add_edges_from(
            itertools.chain.from_iterable(
                (((dep.name, spec.name) for dep in spec.deps.all) for spec in project_specs.values())
            )
        )

        if not _assert_no_app_deps(project_specs.values()):
            raise DAGException("Apps cannot be imported by other apps or libs.")

        if not nx.is_directed_acyclic_graph(graph):
            raise DAGException(
                "A circular dependency between libs exists, ex: two libs cannot have each other as dependencies."
            )

        root = next(nx.topological_generations(graph))
        if len(root) > 1 or root[0] != DAG_ROOT:
            raise DAGException(f"{DAG_ROOT} must be the only root of the dag.")

        repo_dag = RepoDAG(project_specs, changes, graph)

        if save:
            with open(dag_file, "w") as f:
                json.dump(converter.unstructure(repo_dag), f)
                f.write("\n")  # Add newline because Py JSON does not

        return repo_dag
