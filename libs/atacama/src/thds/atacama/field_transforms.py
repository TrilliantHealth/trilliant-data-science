import typing as ty
from copy import deepcopy

import marshmallow  # type: ignore

FieldTransform = ty.Callable[[marshmallow.fields.Field], marshmallow.fields.Field]


def apply_field_xfs(
    field_transforms: ty.Sequence[FieldTransform], fields: ty.Dict[str, marshmallow.fields.Field]
) -> ty.Dict[str, marshmallow.fields.Field]:
    def apply_all(field):
        for fxf in field_transforms:
            field = fxf(deepcopy(field))
        return field

    return {name: apply_all(field) for name, field in fields.items()}
