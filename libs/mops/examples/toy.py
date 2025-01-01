#!/usr/bin/env python3
from functools import partial
from pathlib import Path

from thds.adls.defaults import env_root
from thds.mops import pure

memo = pure.memoize_in(env_root)  # usually thdsscratch/tmp


@memo
def mul2(i: int) -> int:
    return i * 2


# Call mul2 once, this will memoize the output of the mul2 called with the value `2`
print(mul2(2))

# Any subsequent calls to mul2 with the same argument will use the memoized value
print(mul2(2))  # Uses memoized value
print(mul2(2))  # Uses memoized value
print(mul2(3))  # New argument, function will be executed


@memo
def find_in_file(path: Path, s: str) -> bool:
    """Checks if a string is present in a given file"""
    with path.open() as f:
        return s in f.read()


# Use a pipeline ID mask to ensure all functions within this context use the same memoized data across different runs
# of the same pipeline. This prevents the creation of new memoized data for identical operations, ensuring efficiency
# and consistent reuse of results
with pure.pipeline_id_mask("examples"):
    find_in_toy_py = partial(find_in_file, Path(__file__))

    print(find_in_toy_py("mul2"))  # True since "mul2" is in this file
    print(find_in_toy_py("foobar"))  # True
    print(find_in_toy_py("J" + "o" + "e"))  # False as "Joe" is not in this file
