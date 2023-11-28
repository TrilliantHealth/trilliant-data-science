import typing as ty

OBJECT, STRING, INTEGER, NUMBER, ARRAY, BOOLEAN, NULL = (
    "object",
    "string",
    "integer",
    "number",
    "array",
    "boolean",
    "null",
)
TYPE = "type"
TITLE, DESCRIPTION = "title", "description"

_missing = object()


##################################
# Jsonschema object constructors #
##################################


def null():
    return {TYPE: NULL}


def boolean():
    return {TYPE: BOOLEAN}


def string(minLength=_missing, maxLength=_missing, pattern=_missing, format=_missing):
    return _supplied_kwargs(locals(), {TYPE: STRING})


def integer(
    minimum=_missing,
    maximum=_missing,
    exclusiveMinimum=_missing,
    exclusiveMaximum=_missing,
):
    return _numeric(type=INTEGER, **locals())


def number(
    minimum=_missing,
    maximum=_missing,
    exclusiveMinimum=_missing,
    exclusiveMaximum=_missing,
):
    return _numeric(type=NUMBER, **locals())


def array(
    minItems=_missing,
    maxItems=_missing,
    uniqueItems=_missing,
    prefixItems=_missing,
    items=_missing,
):
    return _supplied_kwargs(locals(), {TYPE: ARRAY})


def object_(
    properties=_missing,
    patternProperties=_missing,
    required=_missing,
    minProperties=_missing,
    maxProperties=_missing,
    additionalProperties=_missing,
):
    return _supplied_kwargs(locals(), {TYPE: OBJECT})


def _numeric(
    type: str,
    minimum=_missing,
    maximum=_missing,
    exclusiveMinimum=_missing,
    exclusiveMaximum=_missing,
):
    return _supplied_kwargs(locals())


def _supplied_kwargs(kwargs, namespace: ty.Optional[ty.Dict[str, ty.Any]] = None):
    supplied_kwargs = ((k, v) for k, v in kwargs.items() if v is not _missing)
    if namespace is None:
        return dict(supplied_kwargs)
    else:
        namespace.update(supplied_kwargs)
        return namespace
