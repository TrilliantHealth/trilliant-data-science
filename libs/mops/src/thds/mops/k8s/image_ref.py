"""Optional abstraction for sharing state between a image generating
script and an image-consuming script, e.g. docker-tools/build_push.py
and your application which used to read from an environment variable
that you keep forgetting to set.
"""
import os
from pathlib import Path
from typing import Optional

from thds.core.log import getLogger

from .image_backoff import YIKES
from .launch import autocr

logger = getLogger(__name__)


class ImageFileRef:
    def __init__(self, path: Path):
        self._path = path.resolve()
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
        logger.info(f"In {self._path.parent}:\necho '{fully_qualified_image_name}' > {self._path.name}")
        with open(self._path, "w") as f:
            f.write(fully_qualified_image_name + "\n")


MOPS_IMAGE_FULL_TAG = "MOPS_IMAGE_FULL_TAG"
STD_MOPS_IMAGE_FILE_REF = ImageFileRef(Path(".mops-image-name"))


def std_find_image_full_tag(
    project_name: str = "",
    image_basename: str = "",
    file_ref: ImageFileRef = STD_MOPS_IMAGE_FILE_REF,
):
    """Looks in the 'standard' places in a standard order to try to
    come up with a fully-qualified image tag.

    Enivronment variables are preferred over the ImageFileRef, although
    ImageFileRef is the recommended solution for most applications.
    """
    image_fullref_from_env = os.getenv(MOPS_IMAGE_FULL_TAG)
    if image_fullref_from_env:
        return image_fullref_from_env

    # this type of reference is somewhat deprecated because of a confusing name,
    # but we still will look for it.
    image_tag_from_env = os.getenv(f"{project_name.upper()}_VERSION")
    if image_tag_from_env and project_name:
        # I don't want to use the plain 'VERSION' env var.
        logger.info(
            YIKES(f"Using image name '{image_tag_from_env}' from old-style environment variable.")
        )
        if not image_basename:
            image_basename = f"ds/{project_name}"
        if image_basename in image_tag_from_env:
            return autocr(image_tag_from_env)
        return autocr(f"{image_basename}:{image_tag_from_env}")

    image_tag_from_file = ImageFileRef(Path(".mops-image-name"))()
    if image_tag_from_file:
        return image_tag_from_file

    return ""
