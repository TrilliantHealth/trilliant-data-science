"""Support custom fields that seem necessary to us."""
import typing as ty

import marshmallow as ma


class Set(ma.fields.List):
    """Marshmallow is dragging their feet on implementing a Set Field.

    https://github.com/marshmallow-code/marshmallow/issues/1549

    But we can implement a simple version that basically works.
    """

    def _serialize(self, value, attr, obj, **kwargs) -> ty.Union[ty.List[ty.Any], None]:
        if value is None:
            return None
        # we run a sort because even though roundtrip doesn't
        # guarantee the same value that was deserialized, we'd like
        # for the serialized output to be stable/deterministic.
        return [self.inner._serialize(each, attr, obj, **kwargs) for each in sorted(value)]

    def _deserialize(self, value, attr, data, **kwargs) -> ty.Set[ty.Any]:  # type: ignore
        val = super()._deserialize(value, attr, data, **kwargs)
        return set(val)
