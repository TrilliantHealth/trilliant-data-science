from thds.attrs_utils.docs import record_class_docs


class Record1:
    """Record1 doc

    :param a: Record1 a
    :param b: Record1 b
    :param c: Record1 c
    :raises TypeError: Record1 raises
    :return str: Record1 return
    """


class Record2(Record1):
    """Record2 doc

    Record2 long desc

    :param b: Record2 b
    :param c: Record2 c
    :param d: Record2 d
    :return int: Record2 return
    """


class Record3(Record2):
    """Record3 doc


    Record3 long desc


    :param a: Record3 a
    :param d: Record3 d
    :param e: Record3 e
    :raises ValueError: Record3 raises 1
    :raises RuntimeError: Record3 raises 2
    """


def test_record_class_docs():
    docs = record_class_docs(Record3)

    expected_params = dict(
        a="Record3 a",
        b="Record2 b",
        c="Record2 c",
        d="Record3 d",
        e="Record3 e",
    )
    expected_return = "Record2 return"
    expected_return_type = "int"
    expected_raises = [("ValueError", "Record3 raises 1"), ("RuntimeError", "Record3 raises 2")]
    expected_short_desc = "Record3 doc"
    expected_long_desc = "Record3 long desc"

    assert {p.arg_name: p.description for p in docs.params} == expected_params
    assert docs.returns is not None
    assert docs.returns.description == expected_return
    assert docs.returns.type_name == expected_return_type
    assert [(r.type_name, r.description) for r in docs.raises] == expected_raises
    assert docs.short_description == expected_short_desc
    assert docs.long_description == expected_long_desc

    expected_short_desc = "Record1 doc\nRecord2 doc\nRecord3 doc"
    expected_long_desc = "Record2 long desc\nRecord3 long desc"
    docs = record_class_docs(Record3, combine_docs="join", join_sep="\n")
    assert docs.short_description == expected_short_desc
    assert docs.long_description == expected_long_desc
