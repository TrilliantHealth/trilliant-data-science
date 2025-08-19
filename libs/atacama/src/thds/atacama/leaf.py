# portions copyright Desert contributors - see LICENSE_desert.md
import datetime
import decimal
import typing as ty
import uuid

import marshmallow
import typing_inspect
from typing_extensions import Protocol

# Default leaf types used by `generate_fields`.
# It is possible to swap in your own definitions via a SchemaGenerator.
LeafType = ty.Union[type, ty.Any]


class LeafTypeMapping(Protocol):
    def __contains__(self, __key: LeafType) -> bool:
        ...  # pragma: nocover

    def __getitem__(self, __key: LeafType) -> ty.Type[marshmallow.fields.Field]:
        ...  # pragma: nocover


FieldType = ty.Type[marshmallow.fields.Field]
FieldT = ty.TypeVar("FieldT", bound=FieldType)


def _field_with_default_kwargs(field: FieldT, **default_kwargs) -> FieldT:
    def fake_field(**kwargs):
        combined = {**default_kwargs, **kwargs}
        return field(**combined)

    return ty.cast(FieldT, fake_field)


def allow_already_parsed(typ: ty.Type, mm_field: FieldT) -> FieldT:
    """For certain types, it's quite common to want to be able to pass
    an already-deserialized version of that item without error,
    especially if you want to be able to use a Schema against both a
    database abstraction (e.g. SQLAlchemy) and an API.

    E.g., there's no need to throw an error if you're expecting a
    datetime string so you can turn it into a datetime, but you get a
    datetime itself.
    """

    def _deserialize(self, value, attr, data, **kwargs):
        if isinstance(value, typ):
            return value
        return mm_field._deserialize(self, value, attr, data, **kwargs)

    return ty.cast(
        FieldT,
        type(
            mm_field.__name__ + "AllowsAlreadyDeserialized",
            (mm_field,),  # inherits from this field type
            dict(_deserialize=_deserialize),
        ),
    )


NoneType = type(None)

NATIVE_TO_MARSHMALLOW: LeafTypeMapping = {
    float: marshmallow.fields.Float,
    int: marshmallow.fields.Integer,
    str: marshmallow.fields.String,
    bool: marshmallow.fields.Boolean,
    datetime.datetime: allow_already_parsed(datetime.datetime, marshmallow.fields.DateTime),
    datetime.time: marshmallow.fields.Time,
    datetime.timedelta: allow_already_parsed(datetime.timedelta, marshmallow.fields.TimeDelta),
    datetime.date: allow_already_parsed(datetime.date, marshmallow.fields.Date),
    decimal.Decimal: allow_already_parsed(decimal.Decimal, marshmallow.fields.Decimal),
    uuid.UUID: marshmallow.fields.UUID,
    ty.Union[int, float]: marshmallow.fields.Number,
    ty.Any: _field_with_default_kwargs(marshmallow.fields.Raw, allow_none=True),
    NoneType: _field_with_default_kwargs(marshmallow.fields.Constant, constant=None, allow_none=True),
}


OptFieldType = ty.Optional[FieldType]
TypeHandler = ty.Callable[[LeafType], OptFieldType]


class DynamicLeafTypeMapping(LeafTypeMapping):
    """May be nested infinitely inside one another, with the outer one taking priority."""

    def __init__(self, base_map: LeafTypeMapping, inorder_type_handlers: ty.Sequence[TypeHandler]):
        self.base_map = base_map
        self.inorder_type_handlers = inorder_type_handlers

    def _try_handlers(self, obj: LeafType) -> OptFieldType:
        for handler in self.inorder_type_handlers:
            field_constructor = handler(obj)
            if field_constructor is not None:
                return field_constructor
        return None

    def __contains__(self, obj: LeafType) -> bool:
        if self._try_handlers(obj):
            return True
        return obj in self.base_map

    def __getitem__(self, obj: object) -> FieldType:
        field = self._try_handlers(obj)
        if field is not None:
            return field
        return self.base_map[obj]


def handle_literals(lt: LeafType) -> OptFieldType:
    if typing_inspect.is_literal_type(lt):
        values = typing_inspect.get_args(lt)
        validator = marshmallow.validate.OneOf(values)
        # the validator is the same no matter what - set membership/equality

        types = [type(val) for val in values]
        if all(typ == types[0] for typ in types):
            # if we can narrow down to a single shared leaf type, use it:
            underlying_type = types[0]
            if underlying_type in NATIVE_TO_MARSHMALLOW:
                return _field_with_default_kwargs(
                    NATIVE_TO_MARSHMALLOW[underlying_type], validate=validator
                )
        # otherwise use the Raw type with the OneOf validator
        return _field_with_default_kwargs(marshmallow.fields.Raw, validate=validator)
    return None


AtacamaBaseLeafTypeMapping = DynamicLeafTypeMapping(NATIVE_TO_MARSHMALLOW, [handle_literals])
