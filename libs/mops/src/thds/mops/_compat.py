# compatibility shims

try:
    import tomllib  # type: ignore [import-not-found] # noqa: F401
except ImportError:
    import tomli as tomllib  # noqa: F401

try:
    import importlib_metadata  # type: ignore [import-not-found] # noqa: F401
except ImportError:
    from importlib import metadata as importlib_metadata  # type: ignore[no-redef] # noqa: F401
