"""Allow recursive control of Schema generation.

If you're looking for Schema load/dump behavior, that belongs to
Marshmallow itself and can consequently be configured via _meta.py.

"""
import typing as ty

from thds.core.stack_context import StackContext  # this is our only 'dependency' on core.


class _GenConfig(ty.NamedTuple):
    """Do not construct these directly; they are an implementation detail and subject to change."""

    require_all: bool
    schema_name_suffix: str


def config(require_all: bool = False, schema_name_suffix: str = "") -> _GenConfig:
    """Create a Schema Generation Config.

    :param require_all: The Schema will enforce `required` for all
      attributes on load. This can also be used to generate a
      dump-only schema that accurately describes the way that all
      attributes are 'require_all' to be present upon dump, since
      OpenAPI and JSON schema do not provide a way to distinguish
      between the semantics of "input required" and "output
      require_all".

    :param schema_name_suffix: does what it says on the tin. Sometimes
      you want to generate a different Schema from the same class and
      you don't want the generated suffixes that Marshmallow gives
      you.

    """
    return _GenConfig(require_all=require_all, schema_name_suffix=schema_name_suffix)


PerGenerationConfigContext = StackContext(
    "atacama-per-generation-config-context",
    config(),
)
