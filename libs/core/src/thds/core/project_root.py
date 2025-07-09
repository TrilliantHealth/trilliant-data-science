from pathlib import Path


def _find_project_root(start: Path, anchor_file_name) -> Path:
    """Try to find the project root path by traversing back from the starting path and finding the anchor file"""
    project_root = start.resolve()

    if not project_root.is_dir():
        project_root = project_root.parent
    while not (project_root / anchor_file_name).exists():
        project_root = project_root.parent
        if project_root == Path("/"):
            raise ValueError("Unable to find project root")

    return project_root


def find_project_root(start: Path, anchor_file_name: str = "pyproject.toml") -> Path:
    """
    Try to find the project root, identified as the first ancestor dir containing an "anchor" file.
    If not found, return '/'.
    """
    try:
        return _find_project_root(start, anchor_file_name)
    except ValueError:
        return Path("/")
