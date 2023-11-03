#!/usr/bin/env python3
from functools import partial
from pathlib import Path

from thds.adls.defaults import env_root
from thds.mops import pure

memo = pure.memoize_in(env_root)  # usually thdsscratch/tmp


@memo
def mul2(i: int) -> int:
    return i * 2


print(mul2(2))


@memo
def find_in_file(path: Path, s: str) -> bool:
    with path.open() as f:
        return s in f.read()


find_in_toy_py = partial(find_in_file, Path(__file__))
print(find_in_toy_py("mul2"))  # True
print(find_in_toy_py("foobar"))  # True
print(find_in_toy_py("J" + "o" + "e"))  # False
