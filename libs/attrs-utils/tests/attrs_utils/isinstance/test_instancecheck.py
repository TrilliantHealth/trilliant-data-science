import typing as ty

import pytest

from thds.attrs_utils.isinstance import instancecheck

from .. import conftest


@pytest.mark.parametrize(
    "type",
    conftest.TEST_TYPES,
)
def test_instancecheck(type: ty.Type):
    # check that a check can be constructed
    check = instancecheck(type)
    assert not check(object())
