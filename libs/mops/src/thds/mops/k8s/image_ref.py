"""Optional abstraction for sharing state between a image generating
script and an image-consuming script, e.g. docker-tools/build_push.py
and your application which used to read from an environment variable
that you keep forgetting to set.
"""
from pathlib import Path
from typing import Optional


class ImageFileRef:
    def __init__(self, path: Path):
        self._path = path
        self._cached_image_ref: Optional[str] = None

    def __call__(self) -> str:
        if self._cached_image_ref is not None:
            return self._cached_image_ref
        if not self._path.exists():
            self._cached_image_ref = ""
            return ""
        with open(self._path) as f:
            self._cached_image_ref = f.read().strip("\n ")
            return self._cached_image_ref

    def create(self, fully_qualified_image_name: str):
        assert fully_qualified_image_name, "Cannot be empty"
        with open(self._path, "w") as f:
            f.write(fully_qualified_image_name + "\n")
