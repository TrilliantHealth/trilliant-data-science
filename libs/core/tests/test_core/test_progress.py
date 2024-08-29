import pytest

from thds.core.progress import calc_report_every


@pytest.mark.parametrize(
    "target_interval, total, sec_elapsed, expected",
    [
        (20, 1, 100, 1),
        (20, 1, 20, 1),
        (20, 1, 10, 2),
        (20, 2, 10, 5),
        (20, 2, 5, 10),
        (20, 400, 10, 1000),
        (20, 800, 20, 1000),
        (20, 1600, 40, 1000),
        (20, 10000, 10, 20000),
        (20, 20000, 20, 20000),
        (20, 30000, 30, 20000),
        (20, 40000, 40, 20000),
    ],
)
def test_basic_even_numbers(target_interval, total, sec_elapsed, expected):
    assert calc_report_every(target_interval, total, sec_elapsed) == expected


@pytest.mark.parametrize(
    "target_interval, total, sec_elapsed, expected",
    [
        (20, 15_423, 10, 50_000),
        (20, 15_423, 20, 20_000),
        (20, 15_423, 30, 10_000),
        (20, 15_423, 40, 10_000),
        (20, 2_342_345, 10, 5_000_000),
        (20, 2_342_345, 20, 2_000_000),
        (20, 500_000, 10, 1_000_000),
        (20, 1_000_000, 20, 1_000_000),
        (20, 10_000_000, 200, 1_000_000),
    ],
)
def test_less_obvious_quantities(target_interval, total, sec_elapsed, expected):
    assert calc_report_every(target_interval, total, sec_elapsed) == expected
