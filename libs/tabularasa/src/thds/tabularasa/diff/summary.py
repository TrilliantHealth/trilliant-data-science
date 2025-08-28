import itertools
import typing as ty

import pandas as pd

from ..schema import metaschema
from ..schema.compilation.sphinx import render_table
from ..schema.constraints import ColumnConstraint, EnumConstraint
from . import data as data_diff
from . import schema as schema_diff

DEFAULT_TABLEFMT = "pipe"
DEFAULT_FLOATFMT = ".6g"


def markdown_list(items: ty.Iterable) -> str:
    return "\n".join(map("- {}".format, items))


def markdown_heading(level: int, text: str) -> str:
    return f"{'#' * level} {text}"


def code_literal(text: str) -> str:
    return f"`{text}`"


def _dropped_and_added(
    kind: str, dropped: ty.Iterable, added: ty.Iterable, heading_level: int = 0
) -> ty.Iterator[str]:
    for title, items in (f"{kind} Dropped:", dropped), (f"{kind} Added:", added):
        if items:
            yield markdown_heading(heading_level + 1, title)
            yield markdown_list(items)


def _py_type(dtype: metaschema.ResolvedDType):
    if isinstance(dtype, schema_diff._CUSTOM_DTYPES):
        return dtype.type.python
    return dtype.python


def _constraint_expr(constraint: ColumnConstraint) -> ty.Optional[str]:
    expr = constraint.comment_expr()
    if expr is None and isinstance(constraint, EnumConstraint):
        ordered = "ordered " if constraint.ordered else ""
        return f"{ordered}enum of cardinality {len(constraint.enum)}"
    return expr


def _prepend_sparse_col(
    value: ty.Any, rows: ty.Iterable[ty.Tuple[ty.Any, ...]]
) -> ty.List[ty.Tuple[ty.Any, ...]]:
    return [(v, *row) for v, row in zip(itertools.chain([value], itertools.repeat("")), rows)]


def markdown_column_diff_table(column_diff: schema_diff.ColumnDiff) -> ty.List[ty.Tuple[ty.Any, ...]]:
    rows = []
    compat_note = "" if column_diff.compatible else " (INCOMPATIBLE)"
    if column_diff.before.name != column_diff.after.name:
        rows.append(("Name", column_diff.before.name, column_diff.after.name))
    if nullability := column_diff.nullability_diff:
        rows.append(("Nullability", (~nullability).name, nullability.name))
    if dtype_diff := column_diff.dtype_diff:
        if (bt := dtype_diff.before.parquet) != (at := dtype_diff.after.parquet):
            rows.append((f"Arrow Type{compat_note}", str(bt), str(at)))
        if (bt_ := _py_type(dtype_diff.before)) != (at_ := _py_type(dtype_diff.after)):
            rows.append((f"Python Type{compat_note}", str(bt_), str(at_)))
    if dtype_diff.constraints_dropped:
        rows.extend(
            _prepend_sparse_col(
                "Constraint Dropped", ((_constraint_expr(c), "") for c in dtype_diff.constraints_dropped)
            )
        )
    if dtype_diff.constraints_added:
        rows.extend(
            _prepend_sparse_col(
                "Constraint Added", (("", _constraint_expr(c)) for c in dtype_diff.constraints_added)
            )
        )
    if enum_diff := dtype_diff.enum_diff:
        if enum_diff.values_dropped:
            rows.extend(
                _prepend_sparse_col("Enum Value Dropped", ((v, "") for v in enum_diff.values_dropped))
            )
        if enum_diff.values_added:
            rows.extend(
                _prepend_sparse_col("Enum Value Added", (("", v) for v in enum_diff.values_added))
            )
        if enum_diff.ordered_diff:
            rows.append(
                (
                    "Enum Orderability",
                    (~enum_diff.ordered_diff).name,
                    enum_diff.ordered_diff.name,
                )
            )
        if enum_diff.order_changed:
            rows.append(("Enum Value Order", "", ""))
    return rows


def markdown_table_table_diff_table(table_diff: schema_diff.TableDiff) -> ty.List[ty.Tuple[ty.Any, ...]]:
    rows = []
    rows.extend(_prepend_sparse_col("Column Dropped", ((c, "") for c in table_diff.columns_dropped)))
    rows.extend(_prepend_sparse_col("Column Added", (("", c) for c in table_diff.columns_added)))
    if (pkb := table_diff.before.primary_key) != (pka := table_diff.after.primary_key):
        rows.append(("Primary Key", ", ".join(pkb or ()), ", ".join(pka or ())))
    rows.extend(
        _prepend_sparse_col("Index Dropped", ((", ".join(i), "") for i in table_diff.indexes_dropped))
    )
    rows.extend(
        _prepend_sparse_col("Index Added", (("", ", ".join(i)) for i in table_diff.indexes_added))
    )
    return rows


def markdown_table_diff_summary(
    table_diff: schema_diff.TableDiff,
    heading_level: int = 0,
    tablefmt: str = DEFAULT_TABLEFMT,
) -> ty.Iterator[str]:
    if rows := markdown_table_table_diff_table(table_diff):
        yield markdown_heading(heading_level + 1, "Table Modifications:")
        yield render_table(("Change", "Before", "After"), rows, tablefmt=tablefmt)

    rows = []
    for column_name, column_diff in table_diff.column_diffs.items():
        rows.extend(_prepend_sparse_col(column_name, markdown_column_diff_table(column_diff)))
    if rows:
        yield markdown_heading(heading_level + 1, "Columns Modified:")
        yield render_table(("Column Name", "Change", "Before", "After"), rows, tablefmt=tablefmt)


def markdown_schema_diff_summary(
    schema_diff: schema_diff.SchemaDiff,
    table_predicate: ty.Optional[ty.Callable[[metaschema.Table], bool]] = None,
    heading_level: int = 0,
    tablefmt: str = DEFAULT_TABLEFMT,
) -> ty.Iterator[str]:
    yield from _dropped_and_added(
        "Tables",
        (
            schema_diff.tables_dropped
            if table_predicate is None
            else {n: t for n, t in schema_diff.tables_dropped.items() if table_predicate(t)}
        ),
        (
            schema_diff.tables_added
            if table_predicate is None
            else {n: t for n, t in schema_diff.tables_dropped.items() if table_predicate(t)}
        ),
        heading_level,
    )
    heading = False
    for table_name, table_diff in sorted(schema_diff.table_diffs.items(), key=lambda x: x[0]):
        if (table_predicate is None or table_predicate(table_diff.after)) and table_diff:
            if not heading:
                yield markdown_heading(heading_level + 1, "Tables Modified:")
                heading = True
            yield markdown_heading(heading_level + 2, table_name)
            yield from markdown_table_diff_summary(table_diff, heading_level + 2, tablefmt=tablefmt)


def _floatfmt_from_df(df: pd.DataFrame, floatfmt: str) -> ty.List[ty.Optional[str]]:
    return [floatfmt if dt.kind == "f" else None for dt in df.dtypes.values]


def markdown_dataframe_diff_summary(
    dataframe_diff: data_diff.DataFrameDiff,
    table_name: ty.Optional[str] = None,
    verbose: bool = False,
    value_detail: bool = False,
    value_detail_min_count: int = 0,
    heading_level: int = 0,
    tablefmt: str = DEFAULT_TABLEFMT,
    floatfmt: str = DEFAULT_FLOATFMT,
) -> ty.Iterator[str]:
    heading = False
    table_changes = dataframe_diff.summary()
    if table_changes:
        if table_name:
            yield markdown_heading(heading_level + 1, code_literal(table_name))
            heading = True
        yield markdown_heading(heading_level + 2, "Key Changes:")
        table = table_changes.table().reset_index()
        yield table[table["count"] > 0].to_markdown(
            index=False, tablefmt=tablefmt, floatfmt=_floatfmt_from_df(table, floatfmt)
        )

    meta_changes = dataframe_diff.meta_diff
    if meta_changes is not None and len(meta_changes):
        if table_name and not heading:
            yield markdown_heading(heading_level + 1, code_literal(table_name))
            heading = True
        yield markdown_heading(heading_level + 2, "Metadata Changes:")
        yield meta_changes.to_markdown(
            index=True, tablefmt=tablefmt, floatfmt=_floatfmt_from_df(meta_changes, floatfmt)
        )

    def _drop_zero_cols(df: ty.Optional[pd.DataFrame]) -> ty.Optional[pd.DataFrame]:
        if df is None:
            return None
        nonzero_cols = df.any()
        return df[nonzero_cols.index[nonzero_cols]]

    value_changes = (
        dataframe_diff.row_diff_patterns()
        if verbose
        else _drop_zero_cols(dataframe_diff.column_diff_summary())
    )
    if value_changes is not None and len(value_changes):
        if table_name and not heading:
            yield markdown_heading(heading_level + 1, code_literal(table_name))
        yield markdown_heading(heading_level + 2, "Value Changes:")
        value_changes = value_changes.reset_index()
        yield ty.cast(
            str,
            value_changes.to_markdown(
                index=False, tablefmt=tablefmt, floatfmt=_floatfmt_from_df(value_changes, floatfmt)
            ),
        )
        if value_detail:
            pos_col_diffs = (
                (col_name, col_diff)
                for col_name, col_diff in dataframe_diff.column_diffs.items()
                if col_diff
            )
            for col_name, col_diff in pos_col_diffs:
                col_heading = False
                for kind, prop in (
                    ("Nulled", data_diff.ColumnDiff.nulled_counts),
                    ("Filled", data_diff.ColumnDiff.filled_counts),
                    ("Updated", data_diff.ColumnDiff.updated_counts),
                ):
                    counts = prop.__get__(col_diff)
                    # evaluate these lazily to allow for rendering as they're computed
                    if value_detail_min_count:
                        counts = counts[counts >= value_detail_min_count]
                    if len(counts):
                        if not col_heading:
                            yield markdown_heading(
                                heading_level + 2, f"Column {code_literal(col_name)} Changes Detail:"
                            )
                            col_heading = True
                        yield markdown_heading(heading_level + 3, f"{kind}:")
                        yield counts.to_frame("count").reset_index().to_markdown(
                            index=False, tablefmt=tablefmt
                        )
