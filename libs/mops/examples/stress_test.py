#!/usr/bin/env python
"""Must be run in an environment where thds.mops is importable and
Azure auth is available.

Use this script to experiment with different values for
adls_max_clients and to stress-test any changes made to MemoizingPicklingRunner.
"""

import logging

from thds.mops import config
from thds.mops.pure.tools.stress import stress


def log_debug(handler: logging.Handler, logger: str = "urllib3") -> None:
    std_formatter = logging.getLogger().handlers[0].formatter
    assert std_formatter
    handler.setFormatter(std_formatter)
    logging.getLogger().addHandler(handler)
    logging.getLogger(logger).setLevel(logging.DEBUG)


# log_debug(logging.FileHandler("stress.log"))


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--max-clients", "-c", type=int, default=config.max_concurrent_network_ops())
    parser.add_argument("--num-tasks", "-n", type=int, default=100)
    parser.add_argument("--task-time", "-t", type=float, default=5.0)

    args = parser.parse_args()

    stress(args.max_clients, args.num_tasks, args.task_time)


if __name__ == "__main__":
    main()
