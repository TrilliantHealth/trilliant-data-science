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
