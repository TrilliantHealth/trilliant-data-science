# compatibility shims

try:
    import tomllib  # type: ignore [import-not-found] # noqa: F401
except ImportError:
    import tomli as tomllib  # noqa: F401
