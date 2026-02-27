from datetime import date, timedelta

import pytest

from thds.core import log
from thds.tabularasa.schema.files import FileSourceMixin, UpdateFrequency

logger = log.getLogger(__name__)


@pytest.mark.parametrize(
    "update_freq,expected",
    [("Quarterly", 4), ("Yearly", 1), ("Monthly", 12), ("Biannual", 2)],
)
def test_times_data_source_needs_updating(update_freq: UpdateFrequency, expected: int):
    count = 0
    fs = FileSourceMixin(update_frequency=update_freq, last_updated=date(2025, 1, 1))
    for d in (date(2025, 1, 1) + timedelta(days=x) for x in range(366)):
        if fs.needs_update(d):
            logger.info(f"Needs updating. Current date: {d}, last_updated: {fs.last_updated}")
            count += 1
            fs = FileSourceMixin(update_frequency=update_freq, last_updated=d)
    assert count == expected


@pytest.mark.parametrize(
    "update_freq,first_delivery_month,last_updated,current,expected",
    [
        pytest.param(
            "Yearly", 3, date(2025, 3, 26), date(2026, 2, 20), False, id="yearly-mar-not-due-feb"
        ),
        pytest.param("Yearly", 3, date(2025, 3, 26), date(2026, 3, 1), True, id="yearly-mar-due-mar"),
        pytest.param(
            "Quarterly", 2, date(2025, 2, 15), date(2025, 4, 30), False, id="quarterly-feb-not-due-apr"
        ),
        pytest.param(
            "Quarterly", 2, date(2025, 2, 15), date(2025, 5, 1), True, id="quarterly-feb-due-may"
        ),
        pytest.param("Monthly", 1, date(2025, 1, 15), date(2025, 2, 1), True, id="monthly-new-month"),
        pytest.param("Monthly", 1, date(2025, 1, 15), date(2025, 1, 31), False, id="monthly-same-month"),
        pytest.param(
            "Biannual", 4, date(2025, 4, 1), date(2025, 9, 30), False, id="biannual-apr-not-due-sep"
        ),
        pytest.param(
            "Biannual", 4, date(2025, 4, 1), date(2025, 10, 1), True, id="biannual-apr-due-oct"
        ),
    ],
)
def test_needs_update_with_first_delivery_month(
    update_freq: UpdateFrequency,
    first_delivery_month: int,
    last_updated: date,
    current: date,
    expected: bool,
):
    fs = FileSourceMixin(
        update_frequency=update_freq,
        first_delivery_month=first_delivery_month,
        last_updated=last_updated,
    )
    assert fs.needs_update(current) == expected
