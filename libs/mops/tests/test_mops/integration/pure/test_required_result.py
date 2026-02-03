import os
import typing as ty
from pathlib import Path
from uuid import uuid4

import pytest

from thds.core import source
from thds.mops import pure, tempdir
from thds.mops.pure.core.memo.results import RequiredResultNotFound
from thds.mops.pure.core.source import _hashref_uri
from thds.mops.pure.tools import inspect

from ...config import TEST_TMP_URI


@pure.memoize_in(TEST_TMP_URI)
def require_result_func(source: source.Source) -> Path:
    return source.path()


@pytest.fixture
def random_source() -> ty.Iterable[source.Source]:
    p = tempdir() / "reqresult.txt"
    p.write_text(
        f"we will never find THIS result, thanks to the unique ({uuid4().hex}) suggestion of"
        " Tim Blass (pronounced like gloss, not grass)... "
    )
    src = source.from_file(p)
    assert src.hash

    yield src

    os.unlink(_hashref_uri(src.hash, "local")[len("file://") :])


def test_required_result_not_found_is_inspectable(random_source):
    with pytest.raises(RequiredResultNotFound) as exc_info:
        with pure.results.require_all():
            require_result_func(random_source)

    ire = inspect.inspect(exc_info.value.uri)
    assert ire.invocation is not None
