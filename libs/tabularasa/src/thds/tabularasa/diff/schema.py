"""Diffs for schema objects"""

import dataclasses
import enum
import typing as ty
from functools import cached_property, singledispatch

from ..loaders import parquet_util
from ..schema import metaschema
from ..schema.constraints import AnyColumnConstraint
from ..schema.metaschema import Column, Identifier, Schema, Table

_CUSTOM_DTYPES = (metaschema.AnonCustomType, metaschema.CustomType)


class NullabilityDiff(enum.IntEnum):
    """Works as expected with `bool`: bool(NullabilityDiff.NO_CHANGE) == False"""

    NULL = -1
    NO_CHANGE = 0
    NOT_NULL = 1

    def __invert__(self):
        return NullabilityDiff(-self.value)

    @staticmethod
    def from_nullability(nullable_before: bool, nullable_after: bool):
        return (
            NullabilityDiff.NO_CHANGE
            if nullable_before == nullable_after
            else NullabilityDiff.NOT_NULL if nullable_before else NullabilityDiff.NULL
        )


class OrderedDiff(enum.IntEnum):
    """Works as expected with `bool`: bool(OrderedDiff.NO_CHANGE) == False"""

    UNORDERED = -1
    NO_CHANGE = 0
    ORDERED = 1

    def __invert__(self):
        return OrderedDiff(-self.value)

    @staticmethod
    def from_ordered(ordered_before: bool, ordered_after: bool):
        return (
            OrderedDiff.NO_CHANGE
            if ordered_before == ordered_after
            else OrderedDiff.UNORDERED if ordered_before else OrderedDiff.ORDERED
        )


@dataclasses.dataclass
class EnumDiff:
    before: metaschema.EnumConstraint
    after: metaschema.EnumConstraint

    @cached_property
    def ordered_diff(self) -> OrderedDiff:
        return OrderedDiff.from_ordered(self.before.ordered, self.after.ordered)

    @cached_property
    def order_changed(self) -> bool:
        if self.before.ordered and self.after.ordered:
            common_values_before = [v for v in self.before.enum if v in self.after.enum]
            common_values_after = [v for v in self.after.enum if v in self.before.enum]
            return common_values_before != common_values_after
        return False

    @cached_property
    def values_dropped(self) -> metaschema.EnumList:
        # Note that this uses python comparison semantics; changing dtype from int to float e.g.
        # with enum values [1, 2] -> [1.0, 2.0] will not be considered a change. This change would be
        # picked up as a compatibility change in DtypeDiff.
        return ty.cast(metaschema.EnumList, [v for v in self.before.enum if v not in self.after.enum])

    @cached_property
    def values_added(self) -> metaschema.EnumList:
        return ty.cast(metaschema.EnumList, [v for v in self.after.enum if v not in self.before.enum])

    def __bool__(self):
        return (
            bool(self.ordered_diff)
            or bool(self.order_changed)
            or bool(self.values_dropped or self.values_added)
        )


@singledispatch
def _constraints(dtype: metaschema.ResolvedDType) -> ty.List[AnyColumnConstraint]:
    return []


@_constraints.register(metaschema.AnonCustomType)
@_constraints.register(metaschema.CustomType)
def _constraints_custom(
    dtype: ty.Union[metaschema.AnonCustomType, metaschema.CustomType],
) -> ty.List[AnyColumnConstraint]:
    return dtype.constraints


@dataclasses.dataclass
class DtypeDiff:
    before: metaschema.ResolvedDType
    after: metaschema.ResolvedDType

    def _type_compatible(self, level: parquet_util.TypeCheckLevel) -> bool:
        # The compatibility check is asymmetric; we use the `after` type as the `actual` type
        # (since that's what you'll get when you load the data) and the `before` type as the `expected`
        # type. Hence we're checking whether any pre-existing code expecting the `before` type should be
        # expected to still work after the change.
        return parquet_util.pyarrow_type_compatible(
            self.after.parquet,
            expected=self.before.parquet,
            level=level,
        )

    @cached_property
    def compatible(self) -> bool:
        return (
            self._type_compatible(parquet_util.TypeCheckLevel.compatible)
            and (self.enum_diff is None or not self.enum_diff.values_added)
            # new values are a potential compatibility change for any code that is only expecting the old values
        )

    @cached_property
    def same_kind(self) -> bool:
        return self._type_compatible(parquet_util.TypeCheckLevel.same_kind)

    @cached_property
    def constraints_dropped(self) -> ty.List[AnyColumnConstraint]:
        before_constraints = _constraints(self.before)
        after_constraints = _constraints(self.after)
        return [c for c in before_constraints if c not in after_constraints]

    @cached_property
    def constraints_added(self) -> ty.List[AnyColumnConstraint]:
        before_constraints = _constraints(self.before)
        after_constraints = _constraints(self.after)
        return [c for c in after_constraints if c not in before_constraints]

    @cached_property
    def enum_diff(self) -> ty.Optional[EnumDiff]:
        before = self.before.enum
        after = self.after.enum
        if (before is not None) and (after is not None):
            return EnumDiff(before, after)
        return None

    def __bool__(self):
        # we don't consider type changes that don't change the kind of the type to be a meaningful change;
        # usually this is just a storage optimization, e.g. going from int64 to int32
        return (self.before.parquet != self.after.parquet) or bool(
            self.constraints_dropped or self.constraints_added
        )


@dataclasses.dataclass
class ColumnDiff:
    before: Column
    after: Column

    @cached_property
    def nullability_diff(self) -> NullabilityDiff:
        return NullabilityDiff.from_nullability(self.before.nullable, self.after.nullable)

    @cached_property
    def dtype_diff(self) -> DtypeDiff:
        return DtypeDiff(self.before.type, self.after.type)

    @cached_property
    def compatible(self) -> bool:
        return (self.nullability_diff != NullabilityDiff.NULL) and self.dtype_diff.compatible

    def __bool__(self):
        return bool(self.nullability_diff) or bool(self.dtype_diff)


@dataclasses.dataclass
class TableDiff:
    before: Table
    after: Table

    @cached_property
    def before_columns(self) -> ty.Dict[Identifier, Column]:
        return {c.name: c for c in self.before.columns}

    @cached_property
    def after_columns(self) -> ty.Dict[Identifier, Column]:
        return {c.name: c for c in self.after.columns}

    @cached_property
    def columns_dropped(self) -> ty.Dict[Identifier, Column]:
        after_names = self.after_columns
        return {col.name: col for col in self.before.columns if col.name not in after_names}

    @cached_property
    def columns_added(self) -> ty.Dict[Identifier, Column]:
        before_names = self.before_columns
        return {col.name: col for col in self.after.columns if col.name not in before_names}

    @cached_property
    def column_diffs(self) -> ty.Dict[Identifier, ColumnDiff]:
        before_names = self.before_columns
        after_names = self.after_columns
        return {
            name: ColumnDiff(before_names[name], after_names[name])
            for name in set(before_names).intersection(after_names)
        }

    @cached_property
    def indexes_dropped(self) -> ty.List[metaschema.IdTuple]:
        return [ix for ix in self.before.indexes if ix not in self.after.indexes]

    @cached_property
    def indexes_added(self) -> ty.List[metaschema.IdTuple]:
        return [ix for ix in self.after.indexes if ix not in self.before.indexes]

    def __bool__(self):
        return bool(
            self.columns_dropped
            or self.columns_added
            or self.indexes_dropped
            or self.indexes_added
            or self.before.primary_key != self.after.primary_key
            or any(self.column_diffs.values())
        )


@dataclasses.dataclass
class SchemaDiff:
    before: Schema
    after: Schema

    @cached_property
    def tables_dropped(self) -> ty.Dict[Identifier, Table]:
        return {name: t for name, t in self.before.tables.items() if name not in self.after.tables}

    @cached_property
    def tables_added(self) -> ty.Dict[Identifier, Table]:
        return {name: t for name, t in self.after.tables.items() if name not in self.before.tables}

    @cached_property
    def table_diffs(self) -> ty.Dict[Identifier, TableDiff]:
        before_tables = self.before.tables
        after_tables = self.after.tables
        return {
            name: TableDiff(before_tables[name], after_tables[name])
            for name in set(before_tables).intersection(after_tables)
        }

    def __bool__(self):
        return bool(self.tables_dropped or self.tables_added or any(self.table_diffs.values()))
