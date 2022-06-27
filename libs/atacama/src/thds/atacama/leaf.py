# portions copyright Desert contributors - see LICENSE_desert.md
import datetime
import decimal
import typing as ty
import uuid

import marshmallow

# Default leaf types used by `generate_fields`.
# It is possible to swap in your own definitions via a SchemaGenerator.
LeafTypeMapping = ty.Mapping[ty.Union[type, ty.Any], ty.Type[marshmallow.fields.Field]]

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
    ty.Any: marshmallow.fields.Raw,
}
