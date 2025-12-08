def _divide_range(a: int, b: int, *, by: int) -> tuple[int, int]:
    assert a < b
    range_cardinality = b - a + 1
    q, r = divmod(range_cardinality, by)
    return q, r


def derive_partition_min_max(
    *,
    partition_num: int,
    total_num_partitions: int,
    partitioned_range_min: int,
    partitioned_range_max: int,
) -> tuple[int, int]:
    """
    Calculates a particular partition range of an integer range.

    In the event that total_num_partitions does not evenly divide the range from [partitioned_range_min, partitioned_range_max],
    we distribute the remainder by increasing the partition size by 1 until partition n = r.
    beyond that (n >= r), make the partition q-sized.

    example:
        total_num_partitions=5, partitioned_range_min=0, partitioned_range_max=27

        n | min | max | cardinality
        0 | 0   | 5   | 6
        1 | 6   | 11  | 6
        2 | 12  | 17  | 6
        3 | 18  | 22  | 5
        4 | 23  | 27  | 5

    expanded:
        range_cardinality = 28, q = 5, r = 3

        n | min                       | max
        0 | 0 + 0 * (5 + 1) + 0 * (5) | min + (5 + 1) - 1
        1 | 0 + 1 * (5 + 1) + 0 * (5) | min + (5 + 1) - 1
        2 | 0 + 2 * (5 + 1) + 0 * (5) | min + (5 + 1) - 1
        3 | 0 + 2 * (5 + 1) + 1 * (5) | min + (5    ) - 1
        4 | 0 + 2 * (5 + 1) + 2 * (5) | min + (5    ) - 1
                |   |         |    |            \
    explained   |   |         |    |             \
                '- min(n, r)  '- max(0, n - r)    `- (n < r ? (5 + 1) : 5)
                    '- q + 1       '- q
    """
    assert (
        partition_num < total_num_partitions
    ), f"Cannot derive partition range for a partition_num={partition_num} > total_num_partitions={total_num_partitions}"

    q, r = _divide_range(partitioned_range_min, partitioned_range_max, by=total_num_partitions)
    n = partition_num

    num_partitions_with_remainder, num_partitions_without_remainder = min(n, r), max(0, n - r)
    # ^ "up to n"

    partition_min = (
        partitioned_range_min
        + (num_partitions_with_remainder * (q + 1))
        + (num_partitions_without_remainder * q)
    )
    step = (q + 1 if n < r else q) - 1
    partition_max = partition_min + step

    return partition_min, partition_max
