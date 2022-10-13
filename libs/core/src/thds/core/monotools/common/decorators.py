import functools
import os
from pathlib import Path

try:
    import toml
except ImportError:
    raise ModuleNotFoundError(f"'toml' must be installed ('thds.core[dev]') to use code from '{__name__}'.")

from .constants import PYPROJECT_FILE
from .datamodels import ProjectSpec
from .util import stash_file, path_from_repo
from ...log import getLogger
from ...meta import DEPLOYING, META_FILE, init_metadata, meta_converter
from ...types import StrOrPath

LOGGER = getLogger(__name__)

STASHED_PYRPOJECT_FILE = "pyproject.toml.stashed"


def clean_metadata(path: StrOrPath = "."):
    def _clean_metadata(path: StrOrPath) -> None:
        LOGGER.info("Cleaning local metadata file(s) from '%s'.", path_from_repo(path))
        meta_files = Path(path).rglob(META_FILE)
        for path in meta_files:
            if path.is_file():
                path.unlink()

    def decorator_clean_metadata(func):
        @functools.wraps(func)
        def wrapper_clean_metadata(*args, **kwargs):
            try:
                retval = func(*args, **kwargs)
            except Exception:
                _clean_metadata(path)
                raise
            _clean_metadata(path)
            return retval

        return wrapper_clean_metadata

    return decorator_clean_metadata


def export_metadata(func):
    @functools.wraps(func)
    def wrapper_export_metadata(*args, **kwargs):
        os.environ[DEPLOYING] = "True"
        metadata = meta_converter.unstructure(init_metadata())
        del metadata["misc"]
        LOGGER.info("Exporting metadata:\n" "%s", metadata)
        for k, v in metadata.items():
            if k != "misc":
                os.environ[k.upper()] = str(v)
        return func(*args, **kwargs)

    return wrapper_export_metadata


# This logic in `resolve_pyproject` is heavily inspired by Dan Hipschman of OpenDoor's gist here:
# https://gist.github.com/dan-hipschman-od/070318806610727f17b2d6616ceaa4cc
# The above is referenced in the Q&A from Hipschman's great article here:
# https://medium.com/opendoor-labs/our-python-monorepo-d34028f2b6fa
def resolve_pyproject(path: StrOrPath = ".", incl_patch: bool = True):
    def decorator_resolve_pyproject(func):
        @functools.wraps(func)
        def wrapper_resolve_pyproject(*args, **kwargs):
            pyproject_file = os.path.join(path, PYPROJECT_FILE)
            stashed_pyproject_file = os.path.join(path, STASHED_PYRPOJECT_FILE)

            with stash_file(pyproject_file, stashed_pyproject_file):
                project_spec = ProjectSpec.from_pyproject(stashed_pyproject_file)
                resolved_pyproject = project_spec.resolve_pyproject(incl_patch=incl_patch)

                with open(pyproject_file, "w") as output_file:
                    toml.dump(resolved_pyproject, output_file)

                return func(*args, **kwargs)

        return wrapper_resolve_pyproject

    return decorator_resolve_pyproject
