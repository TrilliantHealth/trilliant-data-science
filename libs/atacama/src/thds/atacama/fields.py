# portions copyright Desert contributors - see LICENSE_desert.md
import collections
import enum
import inspect
import typing as ty
from functools import partial

import marshmallow  # type: ignore
import typing_inspect  # type: ignore

from . import custom_fields
from .leaf import LeafTypeMapping


class VariadicTuple(marshmallow.fields.List):
    """Homogenous tuple with variable number of entries."""

    def _deserialize(self, *args: object, **kwargs: object) -> ty.Tuple[object, ...]:  # type: ignore[override]
        return tuple(super()._deserialize(*args, **kwargs))


T = ty.TypeVar("T")
NoneType = type(None)


def _only(items: ty.Iterable[T]) -> T:
    """Return the only item in an iterable or raise ValueError."""
    [x] = items
    return x


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
) -> marshmallow.fields.Field:
    """Returns a Marshmallow Field if a leaf type can be resolved,
    or if a leaf type can be 'unwrapped' from a Generic or other wrapper type.

    If no leaf type can be resolved, attempts to construct a Nested Schema.

    Derives certain Field keyword arguments from the unwrapping process.
    """
    # this is the base case recursively - a known leaf type.
    if typ in leaf_types:
        return leaf_types[typ](**field_kwargs)

    gen_field = partial(
        generate_field, leaf_types, schema_generator
    )  # all recursive calls from here on out

    def prefer_field_kwargs(**kwargs):
        """In a couple of cases, we generate some Field kwargs, but we
        also want to prefer whatever may have been manually specified.
        """
        return {**kwargs, **field_kwargs}

    # Generic types
    origin = typing_inspect.get_origin(typ)
    if origin:
        # each of these internal calls to field_for_schema for a Generic is recursive
        arguments = typing_inspect.get_args(typ, True)

        def handle_union(types, **field_kwargs):
            import marshmallow_union  # type: ignore

            return marshmallow_union.Union([gen_field(subtyp) for subtyp in types], **field_kwargs)

        if origin in (
            list,
            ty.List,
            ty.Sequence,
            ty.MutableSequence,
            collections.abc.Sequence,
            collections.abc.MutableSequence,
        ):
            return marshmallow.fields.List(gen_field(arguments[0]), **field_kwargs)
        if origin in (set, ty.Set, ty.MutableSet):
            return custom_fields.Set(gen_field(arguments[0]), **field_kwargs)
        if origin in (tuple, ty.Tuple) and Ellipsis not in arguments:
            return marshmallow.fields.Tuple(  # type: ignore[no-untyped-call]
                tuple(gen_field(arg) for arg in arguments),
                **field_kwargs,
            )
        if origin in (tuple, ty.Tuple) and Ellipsis in arguments:
            return VariadicTuple(
                gen_field(_only(arg for arg in arguments if arg != Ellipsis)),
                **field_kwargs,
            )
        if origin in (
            dict,
            ty.Dict,
            ty.Mapping,
            ty.MutableMapping,
            collections.abc.Mapping,
            collections.abc.MutableMapping,
        ):
            return marshmallow.fields.Dict(
                **prefer_field_kwargs(keys=gen_field(arguments[0]), values=gen_field(arguments[1]))
            )
        if typing_inspect.is_optional_type(typ):
            # Optionals are a special case of Union. _if_ the union is
            # fully coalesced, we can treat it as a simple field.
            non_none_subtypes = tuple(t for t in arguments if t is not NoneType)
            if len(non_none_subtypes) == 1:
                # Treat single-argument optional types as a field with a None default
                return gen_field(non_none_subtypes[0], prefer_field_kwargs(allow_none=True))
            # Otherwise, we must fall back to handling it as a Union.
            return handle_union(non_none_subtypes, **prefer_field_kwargs(allow_none=True))
        if typing_inspect.is_union_type(typ):
            return handle_union(arguments)

    # ty.NewType returns a function with a __supertype__ attribute
    newtype_supertype = getattr(typ, "__supertype__", None)
    if newtype_supertype and inspect.isfunction(typ):
        # this is just an unwrapping step.
        # metadata.setdefault("description", typ.__name__)
        return gen_field(newtype_supertype, field_kwargs)

    # enumerations
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
    return marshmallow.fields.Nested(nested_schema, **field_kwargs)
