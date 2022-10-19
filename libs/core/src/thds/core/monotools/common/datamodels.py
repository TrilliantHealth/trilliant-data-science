import os
import typing as ty
from datetime import datetime, timezone
from pathlib import Path

import attr
import cattr
import networkx as nx

ChangeType = ty.Literal["self", "deps", ""]

try:
    import toml
except ModuleNotFoundError:
    raise ModuleNotFoundError(
        f"'toml' must be installed ('thds.core[dev]') to use code from '{__name__}'."
    )

from ...types import StrOrPath
from .constants import PIPFILE, PYPROJECT_FILE
from .util import md5_string

PipfileDepType = ty.Literal["packages", "dev-packages"]


@attr.frozen(hash=True)
class PySpec:
    name: str
    version: str
    path: Path

    @classmethod
    def from_pyproject_file(cls, pyproject_file: StrOrPath) -> "PySpec":
        data = toml.load(str(pyproject_file))
        name = data["project"]["name"]
        version = data["project"]["version"]
        path = os.path.normpath(os.path.dirname(pyproject_file))

        return cls(name=name, version=version, path=Path(path))


@attr.frozen(hash=True)
class DependencySpec(PySpec):
    _extras: ty.Set[str] = attr.field(factory=set, eq=False)

    @classmethod
    def from_pyproject_file(
        cls, pyproject_file: StrOrPath, extras: ty.Optional[ty.Iterable[str]] = None
    ) -> "DependencySpec":
        spec = super().from_pyproject_file(pyproject_file)

        return cls(
            name=spec.name,
            version=spec.version,
            path=spec.path,
            extras=set(extras) if extras else set(),
        )

    @property
    def extras(self) -> ty.List[str]:
        return list(self._extras)

    def as_constraint(self) -> str:
        # Replace local dependencies with published dependencies
        if self.version.count(".") != 1:
            raise ValueError(f"Version in the {PYPROJECT_FILE} at '{self.path}' must be major.minor")

        version_parts = tuple(int(v) for v in self.version.split("."))
        next_minor_version = ".".join([str(version_parts[0]), str(version_parts[1] + 1)])
        # The requirement should be to use the most recent published version, but
        # add a max version as_constraint so we don't pull in future versions that might
        # accidentally break things.
        version_requirement = f">={self.version},<{next_minor_version}"

        return f"{self.name}{self.extras if self._extras else ''}{version_requirement}".replace(
            " ", ""
        ).replace("'", "")


@attr.frozen
class Dependencies:
    runtime: ty.Set[DependencySpec] = attr.field(factory=set)
    dev: ty.Set[DependencySpec] = attr.field(factory=set)

    def add(self, dep: DependencySpec, dep_type: PipfileDepType) -> None:
        if dep_type == "packages":
            self.runtime.add(dep)
        else:
            self.dev.add(dep)

    @property
    def all(self) -> ty.Set[DependencySpec]:
        return self.runtime.union(self.dev)


@attr.frozen(hash=True)
class ProjectSpec(PySpec):
    deps: Dependencies = attr.field(factory=Dependencies, eq=False)

    @classmethod
    def from_pyproject_file(cls, pyproject_file: StrOrPath, incl_extras: bool = True) -> "ProjectSpec":
        spec = super().from_pyproject_file(pyproject_file)
        project_spec = cls(name=spec.name, version=spec.version, path=spec.path)
        project_spec.populate_dependencies(incl_extras=incl_extras)

        return project_spec

    @classmethod
    def from_pyproject_data(
        cls, pyproject_data: ty.Dict, path: StrOrPath, incl_extras: bool = True
    ) -> "ProjectSpec":
        name = pyproject_data["project"]["name"]
        version = pyproject_data["project"]["version"]
        path = path

        project_spec = cls(name=name, version=version, path=Path(path))
        project_spec.populate_dependencies(incl_extras=incl_extras)

        return project_spec

    def release_version(self, incl_patch: bool = True) -> str:
        if self.version.count(".") != 1:
            raise ValueError("Version must be major.minor, patch will be added.")

        patch_version = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S") if incl_patch else ""

        return f"{self.version}.{patch_version}" if patch_version else self.version

    def populate_dependencies(self, incl_extras: bool = True) -> None:
        pipfile_data = toml.load(os.path.join(self.path, PIPFILE))

        for dep_type in ty.get_args(PipfileDepType):
            for dep_name, dep_value in pipfile_data.get(dep_type, {}).items():
                if isinstance(dep_value, dict) and "path" in dep_value and dep_name != self.name:
                    # Found a non-self, local dependency.
                    self.deps.add(
                        DependencySpec.from_pyproject_file(
                            self.path.joinpath(f"{dep_value['path']}/{PYPROJECT_FILE}"),
                            extras=dep_value.get("extras") if incl_extras else None,
                        ),
                        dep_type=dep_type,
                    )

    def resolve_pyproject(self, pyproject_data: ty.Dict, incl_patch: bool = True) -> ty.Dict:
        # TODO - resolving local dev deps and build-time deps

        # Append published verions of local dependencies to pyproject.toml's dependencies
        pyproject_data["project"]["version"] = self.release_version(incl_patch=incl_patch)

        # Append published verions of local dependencies to pyproject.toml's dependencies
        try:
            pyproject_data["project"]["dependencies"].extend(
                (spec.as_constraint() for spec in self.deps.runtime)
            )
        except KeyError:
            pass

        return pyproject_data


@attr.frozen
class RepoDAG:
    _projects: ty.Dict[str, ProjectSpec]
    _changes: ty.Set[str]
    _graph: nx.DiGraph

    def get_project(self, project: str) -> ProjectSpec:
        try:
            return self._projects[project]
        except KeyError:
            raise ValueError(
                f"{project} is not a valid project - available projects: {list(self._projects.keys())}"
            )

    @ty.overload
    def determine_build_order(self) -> ty.List[ty.List[ProjectSpec]]:
        ...  # pragma: no cover

    @ty.overload
    def determine_build_order(self, raw: ty.Literal[False]) -> ty.List[ty.List[ProjectSpec]]:
        ...  # pragma: no cover

    @ty.overload
    def determine_build_order(self, raw: ty.Literal[True]) -> ty.List[ty.List[str]]:
        ...  # pragma: no cover

    def determine_build_order(self, raw: bool = False):
        if raw:
            return list((sorted(gen) for gen in nx.topological_generations(self._graph)))
        return list(
            (
                sorted((self.get_project(project) for project in gen), key=lambda x: x.name)
                for gen in nx.topological_generations(self._graph)
            )
        )

    def get_ancestors(self, project: str) -> ty.List[str]:
        try:
            return nx.ancestors(self._graph, project)
        except nx.NodeNotFound:
            raise ValueError(
                f"'{project}' is not a valid project - available projects: {list(self._projects.keys())}"
            )

    def md5_build_order(self) -> str:
        return md5_string(str(self.determine_build_order(raw=True)))

    def has_changed(self, project: str) -> bool:
        return any(change.startswith(str(self._projects[project].path)) for change in self._changes)

    def change_type(self, project: str) -> ChangeType:
        if self.has_changed(project):
            return "self"
        elif any(self.has_changed(ancestor) for ancestor in nx.ancestors(self._graph, project)):
            return "deps"
        else:
            return ""


converter = cattr.Converter()
converter.register_structure_hook(Path, lambda path, _: Path(path))
converter.register_unstructure_hook(Path, lambda path: str(path))
converter.register_structure_hook(ty.Set[str], lambda s, _: set(s))
converter.register_unstructure_hook(ty.Set[str], lambda s: list(s))
converter.register_structure_hook(
    ty.Set[DependencySpec], lambda deps, _: {converter.structure(dep, DependencySpec) for dep in deps}
)
converter.register_unstructure_hook(
    ty.Set[DependencySpec], lambda deps: [converter.unstructure(dep) for dep in deps]
)
converter.register_structure_hook(nx.DiGraph, lambda graph, _: nx.node_link_graph(graph))
converter.register_unstructure_hook(nx.DiGraph, lambda graph: nx.node_link_data(graph))
converter.register_structure_hook(
    ty.Dict[str, ProjectSpec],
    lambda projects, _: {
        name: converter.structure(spec, ProjectSpec) for name, spec in projects.items()
    },
)
converter.register_unstructure_hook(
    ty.Dict[str, ProjectSpec],
    lambda projects: {name: converter.unstructure(spec) for name, spec in projects.items()},
)
