# portions copyright Desert contributors - see LICENSE_desert.md
import enum
import logging
import typing as ty
from functools import partial

import marshmallow  # type: ignore
import typing_inspect  # type: ignore

from . import custom_fields
from ._generic_dispatch import generic_types_dispatch
from .leaf import LeafTypeMapping


class VariadicTuple(marshmallow.fields.List):
    """Homogenous tuple with variable number of entries."""

    def _deserialize(self, *args: object, **kwargs: object) -> ty.Tuple[object, ...]:  # type: ignore[override]
        return tuple(super()._deserialize(*args, **kwargs))


T = ty.TypeVar("T")
logger = logging.getLogger(__name__)

# most kwargs for a field belong to the surrounding context,
# e.g. the name and the aggregation that this field lives in.
# Therefore, each can be consumed separately from the recursion that happens here.
# One exception is for an Optional field, which is a 'discovery' of allow_none
# that we could not previously have derived.
# The other two exceptions are 'validate', which is applied according to business requirements
# that cannot be derived from the context of the schema, and 'error_messages', which is the same.


def generate_field(
    leaf_types: LeafTypeMapping,
    schema_generator: ty.Callable[[type], ty.Type[marshmallow.Schema]],
    typ: type,
    field_kwargs: ty.Mapping[str, ty.Any] = dict(),  # noqa: [B006]
    debug_name: str = "",
) -> marshmallow.fields.Field:
    """Return a Marshmallow Field or Schema.

    Return a Field if a leaf type can be resolved.or if a leaf type
    can be 'unwrapped' from a Generic or other wrapper type.

    If no leaf type can be resolved, attempts to construct a Nested Schema.

    Derives certain Field keyword arguments from the unwrapping process.
    """
    # this is the base case recursively - a known leaf type.
    if typ in leaf_types:
        return leaf_types[typ](**field_kwargs)

    gen_field = partial(generate_field, leaf_types, schema_generator)
    # all recursive calls from here on out

    def prefer_field_kwargs(**kwargs):
        """In a couple of cases, we generate some Field kwargs, but we
        also want to prefer whatever may have been manually specified.
        """
        return {**kwargs, **field_kwargs}

    def tuple_handler(types: ty.Type, variadic: bool):
        if not variadic:
            return marshmallow.fields.Tuple(  # type: ignore[no-untyped-call]
                tuple(gen_field(typ) for typ in types),
                **field_kwargs,
            )
        return VariadicTuple(gen_field(types[0]), **field_kwargs)

    def union_handler(types, **field_kwargs):
        import marshmallow_union  # type: ignore

        return marshmallow_union.Union([gen_field(subtyp) for subtyp in types], **field_kwargs)

    def optional_handler(non_none_subtypes):
        # Optionals are a special case of Union. _if_ the union is
        # fully coalesced, we can treat it as a simple field.
        if len(non_none_subtypes) == 1:
            # Treat single-argument optional types as a field with a None default
            return gen_field(non_none_subtypes[0], prefer_field_kwargs(allow_none=True))
        # Otherwise, we must fall back to handling it as a Union.
        return union_handler(non_none_subtypes, **prefer_field_kwargs(allow_none=True))

    def fallthrough_handler(typ: ty.Type):
        if type(typ) is enum.EnumMeta:
            import marshmallow_enum  # type: ignore

            return marshmallow_enum.EnumField(typ, **field_kwargs)

        # Nested dataclasses
        forward_reference = typing_inspect.get_forward_arg(typ)
        if forward_reference:
            # TODO this is not getting hit - I think because typing_inspect.get_args
            # resolves all ForwardRefs that live inside any kind of Generic type, including Unions,
            # turning them into _not_ ForwardRefs anymore.a
            return marshmallow.fields.Nested(forward_reference, **field_kwargs)
        # by using a lambda here, we can provide full support for self-recursive schemas
        # the same way Marshmallow itself does:
        # https://marshmallow.readthedocs.io/en/stable/nesting.html#nesting-a-schema-within-itself
        #
        # One disadvantage is that we defer errors until runtime, so it may be worth considering
        # whether we should find a different way of 'discovering' mutually-recursive types
        try:
            nested_schema = schema_generator(typ)
        except RecursionError:
            nested_schema = lambda: schema_generator(typ)  # type: ignore # noqa: E731
        except Exception:
            logger.exception(f"Failed to generate schema for {debug_name}={typ}")
            raise
        return marshmallow.fields.Nested(nested_schema, **field_kwargs)

    return generic_types_dispatch(
        sequence_handler=lambda typ: marshmallow.fields.List(gen_field(typ), **field_kwargs),
        set_handler=lambda typ: custom_fields.Set(gen_field(typ), **field_kwargs),
        tuple_handler=tuple_handler,
        mapping_handler=lambda keytype, valtype: marshmallow.fields.Dict(
            **prefer_field_kwargs(keys=gen_field(keytype), values=gen_field(valtype))
        ),
        optional_handler=optional_handler,
        union_handler=union_handler,
        newtype_handler=lambda newtype_supertype: gen_field(newtype_supertype, field_kwargs),
        fallthrough_handler=fallthrough_handler,
    )(typ)
