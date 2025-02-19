import os
import typing as ty
from pathlib import Path

from thds.core import config, log
from thds.mops._compat import tomllib

logger = log.getLogger(__name__)


def find_first_upward_mops_toml() -> ty.Optional[Path]:
    current = Path.cwd()
    while True:
        try:
            mops_path = current / ".mops.toml"
            if mops_path.is_file() and os.access(mops_path, os.R_OK):
                return mops_path.resolve()
            if current == current.parent:  # At root
                return None
            current = current.parent
        except PermissionError:
            return None


def first_found_config_file() -> ty.Optional[Path]:
    paths = [
        Path(os.environ.get("MOPS_CONFIG", "")),
        find_first_upward_mops_toml(),
        Path(f"{Path.home()}/.mops.toml"),
    ]
    for path in paths:
        if path and path.is_file():
            return path
    return None


def load(config_file: ty.Optional[Path], name: str = "mops") -> ty.Dict[str, ty.Any]:
    if config_file:
        logger.debug("Loading %s config from %s", name, config_file)
        return tomllib.load(open(config_file, "rb"))
    return dict()


max_concurrent_network_ops = config.item("mops.max_concurrent_network_ops", 8, parse=int)
# 8 clients has been obtained experimentally via the `stress_test`
# application running on a Mac M1 laptop running 200 parallel 5 second
# tasks, though no significant difference was obtained between 5 and
# 20 clients. Running a similar stress test from your orchestrator may
# be a good idea if you are dealing with hundreds of micro (<20
# second) remote tasks.

open_files_limit = config.item("mops.resources.max_open_files", 10000)


def _filter_to_known_mops_config(config: ty.Dict[str, ty.Any]) -> ty.Dict[str, ty.Any]:
    return {k: v for k, v in config.items() if k.startswith("mops.") or k.startswith("thds.mops")}


# load this after creating the config items
config.set_global_defaults(_filter_to_known_mops_config(load(first_found_config_file())))
