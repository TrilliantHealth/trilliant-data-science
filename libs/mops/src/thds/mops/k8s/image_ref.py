"""Optional abstraction for sharing state between a image generating
script and an image-consuming script, e.g. docker-tools/build_push.py
and your application which used to read from an environment variable
that you keep forgetting to set.
"""
import os
import subprocess
import typing as ty
from pathlib import Path

from thds.core import lazy, log

from ..colorize import colorized
from .image_backoff import YIKES
from .launch import autocr

logger = log.getLogger(__name__)


PINK = colorized(fg="black", bg="pink")


class ImageFileRef:
    def __init__(self, path: Path):
        self._path = path.resolve()
        self._cached_image_ref: ty.Optional[str] = None

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


def std_docker_build_push_develop_cmd(root: Path) -> ty.List[str]:
    """This is currently a monorepo standard, but it's also a hack.

    The only reason this does not exist in docker-tools is because I
    don't want users having to resort to ImportError hacks when they
    use it, and mops is a runtime dependency whereas docker-tools is
    dev-only and therefore should never be imported inside the
    application.
    """
    script = root / "docker/build_push.py"
    if not script.is_file():
        logger.info(f"We were unable to find {script}; this will not result in an attempt to build.")
        return list()
    return ["poetry", "run", str(script), "--develop"]


def build_push_image(cmd: ty.List[str]) -> ty.Optional[ty.Callable[[], str]]:
    """This is kind of a hack, but it should work for everything
    currently in the monorepo, as well as demandforecast.

    cmd must be runnable by subprocess.run and must return parseable
    stderr lines prefixed with `full_tag: `, of which the last will be
    used.
    """
    if not cmd:
        return None

    def docker_build_push() -> str:
        logger.info(PINK(f"Attempting to build and push docker image with `{' '.join(cmd)}`"))
        # only capture stdout because that's where we expect the full_tag to be,
        # and the rest can get printed to the console for user visibility.
        proc = subprocess.run(cmd, text=True, stdout=subprocess.PIPE)
        if proc.returncode:  # failure!
            print(proc.stdout)
            return ""

        try:
            from thds.docker_tools.build_push import parse_full_tags_from_stdout  # type: ignore

            full_tags = parse_full_tags_from_stdout(proc.stdout)
            if full_tags:
                logger.info(PINK(f"Found {len(full_tags)} built tags; selecting tag '{full_tags[-1]}'"))
                return full_tags[-1]
            logger.warning(YIKES("Build/push command succeeded but no tags were found."))
        except ImportError:
            logger.info(PINK("Unable to parse full tags because thds.docker_tools is not installed."))
        return ""

    # prevent this from running multiple times
    return lazy.Lazy(docker_build_push)


def std_docker_build_push_develop(root: Path) -> ty.Optional[ty.Callable[[], str]]:
    """Easiest possible approach to getting yourself a docker image built and pushed."""
    return build_push_image(std_docker_build_push_develop_cmd(root))


# TODO for a 2.0, make this return a Callable[[], str] instead of str,
# because almost all use cases paired with k8s_shell should be lazily-called.
def std_find_image_full_tag(
    project_name: str = "",
    image_basename: str = "",
    file_ref: ImageFileRef = STD_MOPS_IMAGE_FILE_REF,
    build_push_image: ty.Optional[ty.Callable[[], str]] = None,
):
    """Looks in the 'standard' places in a standard order to try to
    come up with a fully-qualified image tag.

    The priority order is:

    1. environ['MOPS_IMAGE_FULL_TAG']
    2. environ['<PROJECT_NAME.upper()>_VERSION']
    3. non-empty string result of build_push_image, if available.
    4. ImageFileRef

    for production runs, you should export MOPS_IMAGE_FULL_TAG in your
    environment.

    build_push_image is the current recommended solution for most
    development workflows, and the recommended implementation of that
    is std_docker_build_push_develop.

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

    if build_push_image:
        image_tag_from_func = build_push_image()
        if image_tag_from_func:
            return image_tag_from_func

    image_tag_from_file = ImageFileRef(Path(".mops-image-name"))()
    if image_tag_from_file:
        return image_tag_from_file

    logger.warning(YIKES(f"Found no image tag for '{project_name}' '{image_basename}'"))
    return ""
