from importlib import import_module
from importlib.resources import Package

from .meta import get_base_package


def try_imports(*modules: str, module: Package = "", extra: str = "") -> None:
    try:
        for m in modules:
            import_module(m)
    except ImportError:
        if extra and module:
            raise ImportError(
                f"Install the '{extra}' extra for `{get_base_package(module)}` to use `{module}`."
            )
        else:
            raise ImportError(f"Install {list(modules)}{f' to use `{module}`.' if module else ''}")
