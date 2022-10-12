import typing as ty


def load_pyproject(pyproject_file: str) -> ty.Tuple[str, ty.Dict]:
    try:
        import toml
    except ImportError:  # pragma: no cover
        raise RuntimeError("The `toml` package is needed, install `core[dev]`.")
    pyproject_data = toml.load(pyproject_file)
    return pyproject_data["project"]["name"], pyproject_data


def load_pipfile_dependencies(pipfile: str) -> ty.Dict:
    try:
        import toml
    except ImportError:  # pragma: no cover
        raise RuntimeError("The `toml` package is needed, install `core[dev]`.")
    pipfile_data = toml.load(pipfile)
    return pipfile_data.get("packages", {})
