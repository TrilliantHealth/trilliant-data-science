"""Just a utility for keeping actual Storage Account+Container pairs (AdlsRoots) defined
in a central location and referencing those by name throughout your codebase.
"""

import typing as ty

from .fqn import AdlsRoot

_NAMED_ROOTS: ty.Dict[str, AdlsRoot] = dict()


def add(**named_roots: AdlsRoot) -> None:
    """Globally sets some named roots, as a layer of indirection for ADLS URIs."""
    _NAMED_ROOTS.update(named_roots)


def require(name: str) -> AdlsRoot:
    if name not in _NAMED_ROOTS:
        raise ValueError(f"Unknown named root: {name}")

    return _NAMED_ROOTS[name]


def require_uri(name: str) -> str:
    """For use when a system expects a URI rather than the in-house AdlsRoot representation."""
    return str(require(name))
