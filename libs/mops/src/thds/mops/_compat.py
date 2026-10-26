# compatibility shims

try:
    import tomllib  # type: ignore[pyrefly:missing-import] # noqa: F401
except ImportError:
    import tomli as tomllib  # noqa: F401
