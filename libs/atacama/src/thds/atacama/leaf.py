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
        ...

    def __getitem__(self, __key: LeafType) -> ty.Type[marshmallow.fields.Field]:
        ...


FieldType = ty.Type[marshmallow.fields.Field]
FieldT = ty.TypeVar("FieldT", bound=FieldType)


def _field_with_default_kwargs(field: FieldT, **default_kwargs) -> FieldT:
    def fake_field(**kwargs):
        combined = {**default_kwargs, **kwargs}
        return field(**combined)

    return ty.cast(FieldT, fake_field)


NATIVE_TO_MARSHMALLOW: LeafTypeMapping = {
    int: marshmallow.fields.Integer,
    float: marshmallow.fields.Float,
    str: marshmallow.fields.String,
    bool: marshmallow.fields.Boolean,
    datetime.datetime: marshmallow.fields.DateTime,
    datetime.time: marshmallow.fields.Time,
    datetime.timedelta: marshmallow.fields.TimeDelta,
    datetime.date: marshmallow.fields.Date,
    decimal.Decimal: marshmallow.fields.Decimal,
    uuid.UUID: marshmallow.fields.UUID,
    ty.Union[int, float]: marshmallow.fields.Number,
    ty.Any: _field_with_default_kwargs(marshmallow.fields.Raw, allow_none=True),
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
        return _field_with_default_kwargs(
            marshmallow.fields.Raw, validate=marshmallow.validate.OneOf(typing_inspect.get_args(lt))
        )
    return None


AtacamaBaseLeafTypeMapping = DynamicLeafTypeMapping(NATIVE_TO_MARSHMALLOW, [handle_literals])