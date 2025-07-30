from thds.mops.pure.core.memo import calls


def test_callables_resolve():
    def abc():
        pass

    def Def():
        return abc()

    def ghi():
        return Def()

    registry = {
        ghi: [Def],
        Def: [abc],
    }
    assert calls.resolve(registry, ghi) == [Def, abc]
    assert calls.resolve(registry, Def) == [abc]
    assert calls.resolve(registry, abc) == []


def test_callables_resolve_supports_recursion():
    def rec_inner():
        return rec_outer()

    def rec_outer():
        return rec_inner()

    recurse_registry = {
        rec_outer: [rec_inner],
        rec_inner: [rec_outer],
    }
    assert calls.resolve(recurse_registry, rec_outer) == [rec_inner]
    assert calls.resolve(recurse_registry, rec_inner) == [rec_outer]


def test_combine_function_logic_keys():
    def onetwothree():
        """
        function-logic-key: 123
        """

    def fourfive():
        """
        function-logic-key: 45
        """

    def sixseven():
        """
        no function logic key
        """

    results = calls.combine_function_logic_keys((onetwothree, fourfive, sixseven))
    assert results == (
        # the order here is sorted, just so that people don't get weird memoization surprises
        # from minor tweaks that aren't ultimately meant to change the way we consider
        # the function-logic-keys of called functions, all of which are interpreted
        # to apply, but without any order.
        "calls-" + calls.make_unique_name_including_docstring_key(fourfive),
        "calls-" + calls.make_unique_name_including_docstring_key(onetwothree),
    )
