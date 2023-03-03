class MissingAtacamaImports(ImportError):
    pass


try:
    from ._config import config  # noqa: F401
    from ._meta import meta  # noqa: F401
    from .generators import neo, ordered  # noqa: F401
    from .leaf import AtacamaBaseLeafTypeMapping, DynamicLeafTypeMapping  # noqa: F401
    from .schemas import SchemaGenerator  # noqa: F401
except ImportError as iex:
    raise MissingAtacamaImports(
        "You probably need to pipenv install core[atacama] "
        "to make atacama's transitive dependencies available."
    ) from iex
