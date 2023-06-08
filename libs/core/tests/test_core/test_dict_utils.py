import pytest

from thds.core.dict_utils import DotDict, flatten, merge_dicts


def test_already_flatten():
    assert {"a": 1, "b": 2} == flatten({"a": 1, "b": 2})
    assert {"a": None, "b": 2} == flatten({"a": None, "b": 2})
    assert {} == flatten({})


def test_deeply_nested():
    nested_dict = {"a": {"b": {"c": {"d": 1, "e": 2}}}, "f": 3}
    assert {"a.b.c.d": 1, "a.b.c.e": 2, "f": 3} == flatten(nested_dict)


@pytest.mark.parametrize("sep", ["-", "_", " ", "+"])
def test_flatten_with_different_seperators(sep):
    nested_dict = {"a": {"b": {"c": {"d": 1, "e": 2}}}, "f": 3}
    assert {
        f"{sep}".join(["a", "b", "c", "d"]): 1,
        f"{sep}".join(["a", "b", "c", "e"]): 2,
        "f": 3,
    } == flatten(nested_dict, sep=sep)


@pytest.mark.parametrize("convert", [True, False])
def test_dotdict(convert):
    some_dict = {"metrics": {"metric-1": 0.98, "metric-2": {"sub-metric-1": 0.5, "sub-metric-2": 1}}}
    some_dict_converted = {
        "metrics": {"metric_1": 0.98, "metric_2": {"sub_metric_1": 0.5, "sub_metric_2": 1}}
    }
    m = DotDict(some_dict, convert_keys_to_identifiers=convert)
    assert dict(**m) == some_dict if not convert else some_dict_converted
    assert m.to_dict(orig_keys=not convert) == some_dict if not convert else some_dict_converted


def test_dotdict_get():
    m = DotDict({"a_key": 1, "b": 2, "c": {"d": {"e_key": 3, "f": 4}}}, convert_keys_to_identifiers=True)
    assert m.a_key == 1
    assert m.b == 2
    assert m.c.d.e_key == 3
    assert m.c.d.f == 4
    assert m.get_value("a_key") == 1
    assert m.get_value("c.d.f") == 4


def test_dotdict_set():
    m = DotDict({"a_key": 1, "b": 2, "c": {"d": {"e_key": 3, "f": 4}}}, convert_keys_to_identifiers=True)
    m.a_key = 5
    assert m.a_key == 5
    m.c.d = DotDict({"new-data": 100}, convert_keys_to_identifiers=True)
    assert m.c.d.new_data == 100
    m.set_value("a_key", 2)
    assert m.a_key == 2
    m.set_value("c.d.f", 5)
    assert m.c.d.f == 5


@pytest.mark.parametrize(
    "inputs,expected_output",
    [
        (
            [
                {"a": 100, "b": {"c": 200, "d": 300}, "e": [1, 2]},
                {"a": 200, "b": {"c": 300}, "e": [3, 4], "f": 300},
            ],
            {
                "a": [100, 200],
                "b": {"c": [200, 300], "d": [300, None]},
                "e": [[1, 2], [3, 4]],
                "f": [None, 300],
            },
        ),
        ([{}, {}], {}),
        (
            [dict(foo=None), dict(foo=dict(bar=100, baz=200))],
            dict(foo=dict(bar=[None, 100], baz=[None, 200])),
        ),
    ],
)
def test_merge_dicts(inputs, expected_output):
    assert merge_dicts(*inputs) == expected_output
