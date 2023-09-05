import re
import sys
import typing as ty


def module_name_re(modules: ty.Collection[str]) -> ty.Pattern[str]:
    name = "|".join(modules)
    return re.compile(rf"^({name})(?:\.|$)")


def assert_dev_deps_not_imported(import_statement: str, forbidden_modules: ty.Collection[str]):
    """One of the primary features of `mops` is to provide global memoization of pure function calls
    using remote storage mechanisms. Sometimes, as a library author, you'd like to pre-compute the
    result of such a function call, memoizing it and making it available to downstream users without
    requiring them to perform the computation themselves. As such, it is useful to export a public
    interface where such functions can be imported and called to achieve a cache hit and download the
    result locally, _without_ requiring that all the dependencies needed to _compute_ the result be
    present; only `mops` itself need be present to fetch the memoized result. This function can be used
    in your test suite to assert that this condition is met for any import statements that a downstream
    user might use to access your memoized functions.

    :param import_statement: The import statement to test, as a string
    :param forbidden_modules: Module names that should _not_ be imported in the course of executing
      `import_statement`.
    :raises AssertionError: When any of the `forbidden_modules` or their submodules were imported in the
      course of executing `import_statement`
    """
    is_forbidden = module_name_re(forbidden_modules).match
    already_imported = [name for name in sys.modules if is_forbidden(name)]
    to_restore = {name: sys.modules.pop(name) for name in already_imported}
    exec(import_statement, {}, {})
    mistakenly_imported = [name for name in sys.modules if is_forbidden(name)]
    assert (
        not mistakenly_imported
    ), f"Modules {', '.join(mistakenly_imported)} were imported on execution of {import_statement!r}"
    sys.modules.update(to_restore)
