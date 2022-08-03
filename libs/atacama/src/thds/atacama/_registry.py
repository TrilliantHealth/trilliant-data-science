import typing as ty
from collections import defaultdict

import marshmallow

# we maintain our own separate registry that captures all of your arguments.
_SCHEMA_NAME_ONTO_ARGUMENTS_AND_SCHEMA: ty.Dict[
    str, ty.List[ty.Tuple[ty.Tuple[tuple, dict], ty.Type[marshmallow.Schema]]]
] = defaultdict(list)


def only_identical_previously_generated_schema(
    atacama_schema_name: str, *args, **kwargs
) -> ty.Tuple[ty.Optional[ty.Type[marshmallow.Schema]], ty.Callable]:
    """Schemas may be found at various points within a Schema tree,
    and we don't want to regenerate them, partly because this causes
    unnecessary weirdness with Marshmallow's registry logic. This is a
    pretty easy way of avoiding that.

    Schemas are only cached if the arguments to generate that schema,
    including the generator itself, are Python-equal.
    """
    arguments = (args, kwargs)

    def register(schema):
        _SCHEMA_NAME_ONTO_ARGUMENTS_AND_SCHEMA[atacama_schema_name].append((arguments, schema))

    args_and_schemas = _SCHEMA_NAME_ONTO_ARGUMENTS_AND_SCHEMA[atacama_schema_name]
    for schema_arguments, schema in args_and_schemas:
        if schema_arguments == arguments:
            return schema, register
    return None, register
