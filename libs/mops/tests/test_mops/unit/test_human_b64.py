import os
import random

from thds.mops._utils.human_b64 import decode, encode


def test_roundtrip():
    for _ in range(100):
        rnd = os.urandom(100)
        assert decode(encode(rnd, random.randint(1, 100))) == rnd
