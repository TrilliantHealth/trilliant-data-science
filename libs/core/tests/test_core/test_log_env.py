import os
import subprocess
import sys

from thds.core.log import env

_CHILD = """
import os
from thds.core import log
log.getLogger("child").info("working")
"""


def test_as_env_round_trips_through_from_env():
    published = env.as_env(mqitem="train/surgical/zip", mqworker="w-7")
    assert env.from_env(published) == {"mqitem": "train/surgical/zip", "mqworker": "w-7"}


def test_as_env_carries_nothing_but_context():
    assert set(env.as_env(item="x")) == {f"{env.PREFIX}ITEM"}


def test_from_env_ignores_unrelated_variables():
    assert env.from_env({"PATH": "/usr/bin", f"{env.PREFIX}ITEM": "x"}) == {"item": "x"}


def test_suppress_drops_every_context_key_by_default():
    environ = {"PATH": "/usr/bin", **env.as_env(item="x", worker="w-1")}
    assert env.suppress(environ) == {"PATH": "/usr/bin"}


def test_suppress_drops_only_the_named_keys():
    environ = {"PATH": "/usr/bin", **env.as_env(item="x", worker="w-1")}
    remaining = env.suppress(environ, "item")
    assert env.from_env(remaining) == {"worker": "w-1"}
    assert remaining["PATH"] == "/usr/bin"


def test_values_are_stringified_for_the_environment():
    assert env.as_env(attempt=3) == {f"{env.PREFIX}ATTEMPT": "3"}


def _child_log_line(environ: dict) -> str:
    return subprocess.run(
        [sys.executable, "-c", _CHILD],
        env={**environ, "PYTHONPATH": os.pathsep.join(sys.path)},
        capture_output=True,
        text=True,
    ).stderr


def test_a_subprocess_logs_the_context_its_launcher_published():
    """The reason this module exists: a ContextVar cannot reach a child, the environment can."""
    line = _child_log_line({**os.environ, **env.as_env(mqitem="train/surgical/zip")})
    assert "mqitem=train/surgical/zip" in line


def test_a_subprocess_spawned_with_suppress_logs_no_inherited_context():
    launched_with = {**os.environ, **env.as_env(mqitem="train/surgical/zip")}
    assert "mqitem" not in _child_log_line(env.suppress(launched_with))
