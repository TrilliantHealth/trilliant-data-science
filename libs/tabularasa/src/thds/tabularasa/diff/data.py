import dataclasses
import typing as ty
from functools import cached_property

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from ..data_dependencies.adls import sync_adls_data
from ..loaders.util import PandasParquetLoader
from ..schema.files import RemoteBlobStoreSpec
from ..schema.metaschema import Table


def load_historical_data(table: Table, blob_store: RemoteBlobStoreSpec):
    assert table.md5
    loader = PandasParquetLoader.from_schema_table(
        table, package=None, data_dir="", filename=None, derive_schema=False
    )
    remote_data_spec = blob_store.data_spec(table.md5)
    results = sync_adls_data(remote_data_spec)
    assert len(results) == 1
    local_path = results[0].local_path
    meta = pq.read_metadata(local_path)
    return loader(local_path), meta


T_Tabular = ty.TypeVar("T_Tabular", pd.Series, pd.DataFrame)


def _uncategorify_index(data: T_Tabular) -> T_Tabular:
    index = data.index
    if isinstance(index.dtype, pd.CategoricalDtype):
        return data.set_axis(index.astype(index.dtype.categories.dtype), axis=0, copy=False)
    return data


def _uncategorify_series(series: pd.Series) -> pd.Series:
    series = _uncategorify_index(series)
    if isinstance(series.dtype, pd.CategoricalDtype):
        return series.astype(series.dtype.categories.dtype)
    return series


def _uncategorify_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = _uncategorify_index(df)
    update_dtypes = {
        c: dt.categories.dtype for c, dt in df.dtypes.items() if isinstance(dt, pd.CategoricalDtype)
    }
    if not update_dtypes:
        return df
    return df.astype(update_dtypes, copy=False)


def _percent(numerator: int, denominator: int) -> float:
    return numerator * 100 / denominator if denominator else 0.0


class ColumnDiffSummary(ty.NamedTuple):
    nulled: int
    filled: int
    updated: int

    def __bool__(self):
        return any(self)


class DataFrameDiffSummary(ty.NamedTuple):
    rows_before: int
    rows_after: int
    columns_before: int
    columns_after: int
    dropped_rows: int
    added_rows: int
    dropped_columns: int
    added_columns: int

    def __bool__(self):
        return bool(self.dropped_rows or self.dropped_columns or self.added_rows or self.added_columns)

    def table(self):
        rows = [
            ("dropped_rows", self.dropped_rows, _percent(self.dropped_rows, self.rows_before)),
            ("added_rows", self.added_rows, _percent(self.added_rows, self.rows_after)),
            (
                "dropped_columns",
                self.dropped_columns,
                _percent(self.dropped_columns, self.columns_before),
            ),
            ("added_columns", self.added_columns, _percent(self.added_columns, self.columns_after)),
        ]
        return pd.DataFrame.from_records(rows, columns=["", "count", "percent"]).set_index("")


@dataclasses.dataclass
class ColumnDiff:
    before: pd.Series
    after: pd.Series

    def __post_init__(self):
        # to facilitate hassle-free comparison
        self.before = _uncategorify_series(self.before)
        self.after = _uncategorify_series(self.after)

    @cached_property
    def was_null(self) -> pd.Series:
        return self.before.isna()

    @cached_property
    def is_null(self) -> pd.Series:
        return self.after.isna()

    @cached_property
    def nulled(self) -> pd.Series:
        return ~self.was_null & self.is_null

    @cached_property
    def filled(self) -> pd.Series:
        return self.was_null & ~self.is_null

    @cached_property
    def updated(self) -> pd.Series:
        return (self.before != self.after).fillna(False) & ~self.is_null & ~self.was_null

    @cached_property
    def n_nulled(self) -> int:
        return self.nulled.sum()

    @cached_property
    def n_filled(self) -> int:
        return self.filled.sum()

    @cached_property
    def n_updated(self) -> int:
        return self.updated.sum()

    def __bool__(self):
        return bool(self.nulled.any() or self.filled.any() or self.updated.any())

    @cached_property
    def updated_counts(self) -> pd.Series:
        updated = self.updated
        return (
            pd.DataFrame(dict(before=self.before[updated], after=self.after[updated]))
            .value_counts()
            .rename("count", copy=False)
        )

    @cached_property
    def nulled_counts(self) -> pd.Series:
        return (
            self.before[self.nulled]
            .value_counts()
            .rename("count", copy=False)
            .rename_axis(index="before")
        )

    @cached_property
    def filled_counts(self) -> pd.Series:
        return (
            self.after[self.filled].value_counts().rename("count", copy=False).rename_axis(index="after")
        )

    def summary(self):
        return ColumnDiffSummary(nulled=self.n_nulled, filled=self.n_filled, updated=self.n_updated)


@dataclasses.dataclass
class DataFrameDiff:
    before: pd.DataFrame
    after: pd.DataFrame
    before_meta: ty.Optional[pq.FileMetaData] = None
    after_meta: ty.Optional[pq.FileMetaData] = None

    def __post_init__(self):
        self._column_diffs: ty.Dict[str, ColumnDiff] = dict()

    @cached_property
    def dropped_columns(self) -> ty.List[str]:
        return self.before.columns.difference(self.after.columns).tolist()

    @cached_property
    def added_columns(self) -> ty.List[str]:
        return self.after.columns.difference(self.before.columns).tolist()

    @cached_property
    def common_columns(self) -> ty.List[str]:
        return self.after.columns.intersection(self.before.columns).tolist()

    @cached_property
    def dropped_keys(self) -> pd.Index:
        return self.before.index.difference(self.after.index)

    @cached_property
    def added_keys(self) -> pd.Index:
        return self.after.index.difference(self.before.index)

    @cached_property
    def common_keys(self) -> list:
        """Don't use `.index.intersection` here because it does not work with different types of nulls.

        For a single Index, the returned keys are based on the following True statements:
            * None is None
            * pandas.NA is pandas.NA
            * float("nan") is not float("nan")

        For MultiIndex, `float("nan")` behave differently, please check the test for all null equality checks
        E.g.,
        ```
        In [47]: pd.MultiIndex.from_tuples([float("nan")]) == pd.MultiIndex.from_tuples([float("nan")])
        Out[47]: array([ True])

        In [48]: pd.Index([float("nan")]) == pd.Index([float("nan")])
        Out[48]: array([False])
        ```
        """
        return list(set(self.after.index).intersection(self.before.index))

    @cached_property
    def dropped_rows(self) -> pd.DataFrame:
        return self.before.loc[self.dropped_keys]

    @cached_property
    def added_rows(self) -> pd.DataFrame:
        return self.after.loc[self.added_keys]

    @cached_property
    def common_rows_before(self) -> pd.DataFrame:
        return self.before.loc[self.common_keys]

    @cached_property
    def common_rows_after(self) -> pd.DataFrame:
        return self.after.loc[self.common_keys]

    def column_diff(self, column: str) -> ColumnDiff:
        if (maybe_diff := self._column_diffs.get(column)) is None:
            diff = self._column_diffs[column] = ColumnDiff(
                self.common_rows_before[column], self.common_rows_after[column]
            )
            return diff
        return maybe_diff

    @property
    def column_diffs(self) -> ty.Dict[str, ColumnDiff]:
        return {c: self.column_diff(c) for c in self.common_columns}

    def column_diff_summary(self) -> ty.Optional[pd.DataFrame]:
        df = pd.DataFrame.from_dict(
            {name: diff.summary() for name, diff in self.column_diffs.items() if diff},
            orient="index",
            columns=ColumnDiffSummary._fields,
        )
        df.index.name = "column"
        percent_df = df.rename(columns="{}_percent".format, copy=False).applymap(  # type: ignore[operator]
            lambda v: _percent(v, len(self.common_keys))
        )
        df = pd.concat([df, percent_df], axis=1)
        return None if not len(df) else df

    def row_diff_patterns(self, detailed: bool = True) -> ty.Optional[pd.DataFrame]:
        before = _uncategorify_dataframe(self.common_rows_before[self.common_columns])
        after = _uncategorify_dataframe(self.common_rows_after[self.common_columns])
        was_null = before.isna()
        is_null = after.isna()
        filled = was_null & ~is_null
        nulled = ~was_null & is_null
        updated = (before != after).fillna(False) & ~is_null & ~was_null  # type: ignore[attr-defined]
        changed_cols_ = updated.any(axis=0) | nulled.any(axis=0) | filled.any(axis=0)
        changed_cols = changed_cols_.index[changed_cols_].tolist()
        if not changed_cols:
            return None
        if detailed:
            changes = pd.DataFrame(
                np.where(
                    updated[changed_cols].values,
                    "updated",
                    np.where(
                        nulled[changed_cols].values,
                        "nulled",
                        np.where(filled[changed_cols].values, "filled", ""),
                    ),
                ),
                index=updated.index,
                columns=changed_cols,
            ).astype("category")
        else:
            changes = updated[changed_cols] | nulled[changed_cols] | filled[changed_cols]
        changes_df = changes.value_counts(dropna=False).to_frame("count")
        changes_df["percent"] = changes_df["count"].apply(lambda c: _percent(c, len(self.common_keys)))
        return changes_df

    def summary(self) -> DataFrameDiffSummary:
        return DataFrameDiffSummary(
            rows_before=len(self.before),
            rows_after=len(self.after),
            columns_before=len(self.before.columns),
            columns_after=len(self.after.columns),
            dropped_rows=len(self.dropped_keys),
            added_rows=len(self.added_keys),
            dropped_columns=len(self.dropped_columns),
            added_columns=len(self.added_columns),
        )

    @cached_property
    def meta_diff(self):
        if self.before_meta is None or self.after_meta is None:
            return pd.DataFrame(columns=["before", "after"], dtype=object)

        before = self.before_meta.to_dict()
        after = self.after_meta.to_dict()
        return pd.DataFrame.from_dict(
            {
                name: [before[name], after[name]]
                for name in before
                if (name != "row_groups") and (before[name] != after[name])
            },
            orient="index",
            columns=["before", "after"],
            dtype=object,
        )

    def __bool__(self) -> bool:
        return bool(
            len(self.meta_diff)
            or len(self.dropped_keys)
            or len(self.added_keys)
            or len(self.dropped_columns)
            or len(self.added_columns)
            or any(map(bool, map(self.column_diff, self.common_columns)))
        )

    @staticmethod
    def from_tables(
        before: Table,
        after: Table,
        before_blob_store: RemoteBlobStoreSpec,
        after_blob_store: RemoteBlobStoreSpec,
    ) -> "DataFrameDiff":
        before_df, before_meta = load_historical_data(before, before_blob_store)
        after_df, after_meta = load_historical_data(after, after_blob_store)
        return DataFrameDiff(
            before=before_df,
            after=after_df,
            before_meta=before_meta,
            after_meta=after_meta,
        )
