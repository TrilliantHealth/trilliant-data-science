# portions copyright Desert contributors - see LICENSE_desert.md
import collections
import typing as ty

import typing_inspect

NoneType = type(None)


def generic_types_dispatch(
    sequence_handler,
    set_handler,
    tuple_handler,
    mapping_handler,
    optional_handler,
    union_handler,
    newtype_handler,
    fallthrough_handler,
):
    def type_discriminator(typ: ty.Type):
        if origin := typing_inspect.get_origin(typ) or ty.get_origin(
            typ
        ):  # in case typing_inspect fails
            # each of these internal calls to field_for_schema for a Generic is recursive
            arguments = typing_inspect.get_args(typ, True)

            if origin in (
                list,
                ty.List,
                ty.Sequence,
                ty.MutableSequence,
                collections.abc.Sequence,
                collections.abc.MutableSequence,
            ):
                return sequence_handler(arguments[0])
            if origin in (set, ty.Set, ty.MutableSet):
                return set_handler(arguments[0])
            if origin in (tuple, ty.Tuple) and Ellipsis not in arguments:
                return tuple_handler(arguments, variadic=False)
            if origin in (tuple, ty.Tuple) and Ellipsis in arguments:
                return tuple_handler([arguments[0]], variadic=True)
            if origin in (
                dict,
                ty.Dict,
                ty.Mapping,
                ty.MutableMapping,
                collections.abc.Mapping,
                collections.abc.MutableMapping,
            ):
                return mapping_handler(arguments[0], arguments[1])
            if typing_inspect.is_optional_type(typ):
                non_none_subtypes = tuple(t for t in arguments if t is not NoneType)
                return optional_handler(non_none_subtypes)
            if typing_inspect.is_union_type(typ):
                return union_handler(arguments)

        newtype_supertype = getattr(typ, "__supertype__", None)
        if newtype_supertype and typing_inspect.is_new_type(typ):
            # this is just an unwrapping step.
            return newtype_handler(newtype_supertype)

        return fallthrough_handler(typ)

    return type_discriminator
