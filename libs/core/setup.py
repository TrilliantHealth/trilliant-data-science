import sys

from setuptools import setup

sys.path.insert(0, "src")

from thds.core.meta import write_metadata  # noqa: E402

write_metadata("core")

setup()
