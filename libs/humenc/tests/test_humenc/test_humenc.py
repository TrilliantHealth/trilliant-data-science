import os
import random
import typing as ty

import pytest

from thds import humenc


def test_version_at_import():
    assert humenc.__version__


@pytest.mark.parametrize(
    ["altchars", "splitchar"],
    [
        pytest.param(
            humenc.ALTCHARS,
            humenc.SPLITCHAR,
            id=f"altchars={humenc.ALTCHARS!r}, splitchar='{humenc.SPLITCHAR}'",
        ),
        pytest.param(
            humenc.ALTCHARS,
            "=",
            id=f"altchars={humenc.ALTCHARS!r}, splitchar='='",
        ),
        pytest.param(None, humenc.SPLITCHAR, id=f"altchars='None', splitchar='{humenc.SPLITCHAR}'"),
        pytest.param(None, "=", id="altchars='None', splitchar='='"),
    ],
)
def test_roundtrip(altchars: ty.Optional[humenc.Buffer], splitchar: str):
    for _ in range(100):
        rnd = os.urandom(100)
        assert (
            humenc.decode(
                humenc.encode(rnd, random.randint(1, 100), altchars=altchars, splitchar=splitchar),
                altchars=altchars,
                splitchar=splitchar,
            )
            == rnd
        )
