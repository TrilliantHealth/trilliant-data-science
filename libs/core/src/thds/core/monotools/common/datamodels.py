import os
import typing as ty
from copy import deepcopy
from datetime import datetime, timezone

import attr

try:
    import toml
except ImportError:
    raise ModuleNotFoundError(f"'toml' must be installed ('thds.core[dev]') to use code from '{__name__}'.")

from ...types import StrOrPath
from .constants import PIPFILE, PYPROJECT_FILE

PipfileDepType = ty.Literal["packages", "dev-packages"]


@attr.frozen(hash=True)
class PySpec:
    name: str
    version: str
    path: str
    _data: ty.Dict = attr.field(factory=dict, eq=False)

    @classmethod
    def from_pyproject(cls, pyproject_file: StrOrPath) -> "PySpec":
        data = toml.load(str(pyproject_file))
        name = data["project"]["name"]
        version = data["project"]["version"]
        path = os.path.normpath(os.path.dirname(pyproject_file))

        return cls(name=name, version=version, path=path, data=data)

    @property
    def data(self) -> ty.Dict:
        return deepcopy(self._data)


@attr.frozen(hash=True)
class DependencySpec(PySpec):
    _extras: ty.Set[str] = attr.field(factory=set, eq=False)

    @classmethod
    def from_pyproject(
        cls, pyproject_file: StrOrPath, extras: ty.Optional[ty.Iterable[str]] = None
    ) -> "DependencySpec":
        spec = super().from_pyproject(pyproject_file)

        return cls(
            name=spec.name,
            version=spec.version,
            path=spec.path,
            data=spec.data,
            extras=set(extras) if extras else set(),
        )

    @property
    def extras(self) -> ty.List[str]:
        return list(self._extras)

    @property
    def constraint(self) -> str:
        # Replace local dependencies with published dependencies
        if self.version.count(".") != 1:
            raise ValueError(f"Version in the {PYPROJECT_FILE} at '{self.path}' must be major.minor")

        version_parts = tuple(int(v) for v in self.version.split("."))
        next_minor_version = ".".join([str(version_parts[0]), str(version_parts[1] + 1)])
        # The requirement should be to use the most recent published version, but
        # add a max version constraint so we don't pull in future versions that might
        # accidentally break things.
        version_requirement = f">={self.version},<{next_minor_version}"

        return f"{self.name}{self.extras if self._extras else ''}{version_requirement}".replace(
            " ", ""
        ).replace("'", "")


@attr.define
class Dependencies:
    runtime: ty.Set[DependencySpec] = attr.field(factory=set)
    dev: ty.Set[DependencySpec] = attr.field(factory=set)

    def add(self, dep: DependencySpec, dep_type: PipfileDepType) -> None:
        if dep_type == "packages":
            self.runtime.add(dep)
        else:
            self.dev.add(dep)


@attr.frozen(hash=True)
class ProjectSpec(PySpec):
    deps: Dependencies = attr.field(factory=Dependencies, eq=False)

    @classmethod
    def from_pyproject(cls, pyproject_file: StrOrPath, incl_extras: bool = True) -> "ProjectSpec":
        spec = super().from_pyproject(pyproject_file)
        project_spec = cls(name=spec.name, version=spec.version, path=spec.path, data=spec._data)
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
                        DependencySpec.from_pyproject(
                            os.path.join(self.path, f"{dep_value['path']}/{PYPROJECT_FILE}"),
                            extras=dep_value.get("extras") if incl_extras else None,
                        ),
                        dep_type=dep_type,
                    )

    def resolve_pyproject(self, incl_patch: bool = True) -> ty.Dict:
        # TODO - resolving local dev deps and build-time deps
        data = self.data

        # Append published verions of local dependencies to pyproject.toml's dependencies
        data["project"]["version"] = self.release_version(incl_patch=incl_patch)

        # Append published verions of local dependencies to pyproject.toml's dependencies
        try:
            data["project"]["dependencies"].extend((spec.constraint for spec in self.deps.runtime))
        except KeyError:
            pass

        return data
