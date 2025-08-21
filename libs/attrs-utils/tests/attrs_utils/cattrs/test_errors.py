import warnings
from typing import Any, Optional

import attrs
import cattrs
import pytest

from thds.attrs_utils import cattrs as cattrs_utils


@attrs.define
class Bar:
    baz: int
    quux: bool


@attrs.define
class Foo:
    bar: Optional[Bar]
    baz: float = 0.0


@attrs.define
class Record:
    foo: Optional[Foo]
    bar: bool = True


converter = cattrs_utils.setup_converter(
    cattrs_utils.default_converter(forbid_extra_keys=True),
    restricted_conversions=cattrs_utils.DEFAULT_RESTRICTED_CONVERSIONS,
)

record1: Any = dict()

msg1 = """
While structuring Record:
  required field missing @ $.foo
""".strip()

record2: Any = dict(foo=dict())

msg2 = """
While structuring Record:
  required field missing @ $.foo.bar
""".strip()

record3: Any = dict(foo=None, bar=None)

msg3 = """
While structuring Record:
  invalid value for type, expected bool, got NoneType @ $.bar
""".strip()

record4: Any = dict(foo=dict(bar=None, baz="asdf"))

msg4 = """
While structuring Record:
  invalid value for type, expected float @ $.foo.baz
""".strip()

record5: Any = dict(baz=None, foo=dict(foo=None))

msg5 = """
While structuring Record:
  extra fields found (baz) @ $
  extra fields found (foo) @ $.foo
  required field missing @ $.foo.bar
""".strip()

record6: Any = dict(
    foo=dict(
        bar=dict(
            baz=123.4,
            nope="still not a field",
        ),
        baz="not a number",
        quux="not a field",
    ),
    bar=123,
    baz="nope",
)

msg6 = """
While structuring Record:
  extra fields found (baz) @ $
  invalid value for type, expected bool, got int @ $.bar
  extra fields found (quux) @ $.foo
  invalid value for type, expected float @ $.foo.baz
  extra fields found (nope) @ $.foo.bar
  required field missing @ $.foo.bar.quux
  invalid value for type, expected int, got float @ $.foo.bar.baz
""".strip()


def assert_messages_equal_up_to_line_permutation(
    actual: str,
    expected: str,
):
    """Just in case the ordering of exception messages changes in cattrs.transform_error in the future, we can fall
    back to this with a warning."""

    def rejoin_in_order(lines: list[str]) -> str:
        sep = "\n  "
        return f"{lines[0]}{sep}{sep.join(sorted(lines[1:]))}"

    actual_ = rejoin_in_order(actual.splitlines())
    expected_ = rejoin_in_order(expected.splitlines())
    assert actual_ == expected_


@pytest.mark.parametrize(
    "value, expected_msg",
    [
        pytest.param(record1, msg1, id="missing_top_level_field"),
        pytest.param(record2, msg2, id="missing_nested_field"),
        pytest.param(record3, msg3, id="wrong_type_top_level_field"),
        pytest.param(record4, msg4, id="wrong_type_nested_field"),
        pytest.param(record5, msg5, id="extra_fields_multiple_levels"),
        pytest.param(record6, msg6, id="all_possible_errors"),
    ],
)
def test_format_cattrs_classval_error(value, expected_msg: str):
    """Test that the cattrs ClassValidationError is formatted correctly."""
    with pytest.raises(cattrs.errors.ClassValidationError) as exc_info:
        converter.structure(value, Record)

    exc = exc_info.value
    actual_msg = cattrs_utils.format_cattrs_classval_error(exc)
    try:
        assert actual_msg == expected_msg
    except AssertionError:
        warnings.warn(
            "Assertion failed, checking messages equal up to line permutation. Did you upgrade cattrs recently?"
        )
        assert_messages_equal_up_to_line_permutation(actual_msg, expected_msg)
