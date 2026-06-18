"""Logic for determining a primary key for a data diff"""

import dataclasses
import logging

from thds.tabularasa.loaders import parquet_util

from .schema import TableDiff


def data_diffable(table_diff: TableDiff, logger: logging.Logger | None = None) -> TableDiff | None:
    """We can only diff data from two tables if we have a comparable primary key.

    This function attempts to determine such a key. If it can't, it returns None, otherwise it returns a new `TableDiff`
    where the before/after tables have comparable primary keys. The primary key of the new table is always the source of
    truth; it is never altered. Only the prior table's PK may be altered to align.

    The logic is as follows:
    - if the new table lacks a PK, don't assume the old table's PK is a PK for the new data; don't compare.
    - if the new table has a PK and the old table doesn't, attempt to use the new PK to compare
    - if both tables have a PK:
      - attempt to use the new PK to compare, while logging any before -> after PK changes; refuse to guess at
        alignment of old/new PK columns

    At the end of this process, if the old and new tables aren't comparable via the new PK due to missing columns or
    type incompatibilities, return `None`, otherwise return the new `TableDiff` object with the prior table's PK set to
    the new table's PK.
    """
    logger = logger or logging.getLogger(__name__)
    table_name = table_diff.after.name

    if not (pka := table_diff.after.primary_key):
        logger.warning(f"{table_name}: No primary key in current schema; can't diff data")
        return None
    elif not (pkb := table_diff.before.primary_key):
        # just use the new pk
        logger.warning(
            f"{table_name}: No primary key in prior schema; attempting to use current primary key for data diff: {pka}"
        )
    elif pka != pkb:
        # attempt to use new primary key; refuse any attempt to "align" old and new - this block is just for logging
        if len(pka) != len(pkb):
            logger.warning(
                f"{table_name}: Primary key changed length: {pkb} -> {pka}; attempting to use new primary key for diff"
            )
        elif set(pka) == set(pkb):
            # heuristic: if the names are _exactly_ the same, assume they should be ordered the same
            logger.warning(
                f"{table_name}: Primary key changed order: {pkb} -> {pka}; attempting to use new primary key for diff"
            )
        else:
            logger.warning(
                f"{table_name}: Primary key changed: {pkb} -> {pka}; attempting to use new primary key for diff"
            )

    diff_pk = pka
    prior_cols = {c.name: c for c in table_diff.before.columns}
    current_cols = {c.name: c for c in table_diff.after.columns}
    for cols, which in [(prior_cols, "prior"), (current_cols, "current")]:
        if missing := [c for c in diff_pk if c not in cols]:
            logger.warning(
                f"{table_name}: Can't use primary key {diff_pk} on {which} table; columns {missing} are absent"
            )
            return None

    before_pk_cols = [prior_cols[k] for k in diff_pk]
    after_pk_cols = [current_cols[k] for k in diff_pk]
    if incomparable := [
        (c1.name, c2.name)
        for c1, c2 in zip(before_pk_cols, after_pk_cols)
        if not parquet_util.pyarrow_type_compatible(
            c1.type.parquet, c2.type.parquet, parquet_util.TypeCheckLevel.compatible
        )
    ]:
        _incomparable = ", ".join(f"{a} <-> {b}" for a, b in incomparable)
        logger.warning(f"{table_name}: Primary key changed types: {_incomparable}; can't diff")
        return None

    logger.info(f"{table_name}: Diffable on shared primary key {diff_pk}")
    return dataclasses.replace(
        table_diff,
        before=table_diff.before.model_copy(update=dict(primary_key=diff_pk)),
        after=table_diff.after.model_copy(update=dict(primary_key=diff_pk)),
    )
