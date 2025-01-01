"""This entire module is/could be a generic stack-configuration layer
built over top of static application config that allows it to be
selectively overridden on a per-stack/thread basis. It would get moved
to `thds.core` without the `tomli` dependency or the `mops`-specific
config.

I just need to finish abstracting it and give it a nicer API.

"""
import os
import typing as ty
from pathlib import Path

import tomli

from thds.core import config


def _load_first_found_config() -> ty.Dict[str, ty.Any]:
    paths = [
        Path(os.environ.get("MOPS_CONFIG", "")),
        Path(".mops.toml").resolve(),
        Path(f"{Path.home()}/.mops.toml"),
    ]
    for path in paths:
        if path and path.exists() and path.is_file():
            return tomli.load(open(path, "rb"))
    return dict()


max_concurrent_network_ops = config.item("mops.max_concurrent_network_ops", 8, parse=int)
# 8 clients has been obtained experimentally via the `stress_test`
# application running on a Mac M1 laptop running 200 parallel 5 second
# tasks, though no significant difference was obtained between 5 and
# 20 clients. Running a similar stress test from your orchestrator may
# be a good idea if you are dealing with hundreds of micro (<20
# second) remote tasks.

open_files_limit = config.item("mops.resources.max_open_files", 10000)


# load this after creating the config items
config.set_global_defaults(_load_first_found_config())
