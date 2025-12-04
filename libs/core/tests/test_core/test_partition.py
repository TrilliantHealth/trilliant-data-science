import pytest

from thds.core.partition import derive_partition_min_max


@pytest.mark.parametrize(
    "partition_num, total_num_partitions, partitioned_range_min, partitioned_range_max, expected",
    [
        pytest.param(0, 5, 0, 27, (0, 5)),
        pytest.param(4, 5, 0, 27, (23, 27)),
        pytest.param(0, 5, 3, 27, (3, 7)),
    ],
)
def test_derive_partition_min_max(
    partition_num, total_num_partitions, partitioned_range_min, partitioned_range_max, expected
):
    assert (
        derive_partition_min_max(
            partition_num=partition_num,
            total_num_partitions=total_num_partitions,
            partitioned_range_min=partitioned_range_min,
            partitioned_range_max=partitioned_range_max,
        )
        == expected
    )
