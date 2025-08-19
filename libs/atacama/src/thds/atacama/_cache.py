"""A simple equality-based cache for schema generation."""
import typing as ty
from collections import defaultdict
from functools import wraps

import marshmallow

GenSchema = ty.TypeVar("GenSchema", bound=ty.Callable[..., ty.Type[marshmallow.Schema]])
GenSchemaCachingDeco = ty.Callable[[GenSchema], GenSchema]


def attrs_schema_cache() -> GenSchemaCachingDeco:
    """Allows sharing an attrs schema cache across multiple schema generators."""
    schema_name_onto_arguments_and_schema: ty.Dict[
        str, ty.List[ty.Tuple[ty.Tuple[tuple, dict], ty.Type[marshmallow.Schema]]]
    ] = defaultdict(list)

    def attrs_schema_cache_deco(gen_schema: GenSchema) -> GenSchema:
        @wraps(gen_schema)
        def caching_gen_schema(*args, **kwargs):
            # the following two lines are purely an optimization,
            # so we don't have to search through all possible schemas generated.
            schema_typename = str(args[0])
            args_and_schemas = schema_name_onto_arguments_and_schema[schema_typename]

            arguments = (args, kwargs)
            for schema_arguments, schema in args_and_schemas:
                if schema_arguments == arguments:
                    return schema

            schema = gen_schema(*args, **kwargs)
            schema_name_onto_arguments_and_schema[schema_typename].append((arguments, schema))
            return schema

        return ty.cast(GenSchema, caching_gen_schema)

    return attrs_schema_cache_deco
