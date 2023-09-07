from .__about__ import __commit__, __version__  # noqa
from ._utils.temp import tempdir  # noqa


def main():
    """this exists purely because of DBX and for no other reason."""
    from thds.mops.pure.core.entry.main import main

    main()
