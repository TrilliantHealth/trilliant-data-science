import argparse
from pathlib import Path

from ... import __version__

try:
    from .packaging import build, release
except ImportError:
    raise RuntimeError("'thds.core[dev]' must be installed to use the 'distr' CLI.")


def main():
    parser = argparse.ArgumentParser(description="code distribution CLI", prog="distr")
    parser.add_argument("-v", "--version", action="version", version=__version__)
    subparsers = parser.add_subparsers(help="available 'distr' commands")

    package_parser = subparsers.add_parser(
        "package", description="Python packaging commands", help="Python packaging commands"
    )
    package_subparsers = package_parser.add_subparsers(help="available 'package' commands")

    build_parser = package_subparsers.add_parser(
        "build",
        description="builds a Python package ouputting the result to 'path/dist'",
        help="build a Python package",
    )
    build_parser.add_argument("path", type=Path, help="path to the root of the package to build")
    build_parser.set_defaults(func=lambda x: build(x.path))

    release_parser = package_subparsers.add_parser(
        "release",
        description="releases the Python package at 'path/dist' to Artifactory - will build before releasing by default",
        help="release a Python package to Artifactory",
    )
    release_parser.add_argument("path", type=Path, help="path to the root of the package to release")
    release_parser.add_argument(
        "--skip-build",
        action="store_const",
        const=True,
        help="flag - do not build the package before releasing",
    )
    release_parser.set_defaults(func=lambda x: release(x.path, x.skip_build))

    args = parser.parse_args()
    args.func(args)
