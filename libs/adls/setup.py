from setuptools import setup

from thds.core.meta import write_metadata  # noqa: E402

# BUILD SCRIPTING GOES HERE
write_metadata("adls")

setup()
