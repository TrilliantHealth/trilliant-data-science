import os
import sys

from setuptools import setup

# CONVENTION FOR MAKING THE PACKAGE AVAILABLE TO ITSELF AT BUILD TIME
sys.path.insert(0, "src")
from thds.core.meta import write_metadata  # noqa: E402

# BUILD SCRIPTING GOES HERE
write_metadata("core")

# ALL DECLARATIVE SETUP GOES IN THE pyproject.toml
build_name = os.getenv("BUILD_NAME", "")
build_base = f"build/{build_name}" if build_name else "build"
# SOLVES PARALLEL PIPENV OPERATIONS RACE CONDITION
setup(options={"build": {"build_base": build_base}})
