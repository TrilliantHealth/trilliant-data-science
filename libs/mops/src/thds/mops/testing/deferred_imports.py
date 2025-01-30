import ast
import itertools
import re
import sys
import typing as ty
from contextlib import contextmanager

from thds.core.log import getLogger


def module_name_re(modules: ty.Collection[str]) -> ty.Pattern[str]:
    name = "|".join(modules)
    return re.compile(rf"^({name})(?:\.|$)")


def module_names_from_import_statement(import_stmt: str) -> ty.Set[str]:
    statements = ast.parse(import_stmt).body

    def _extract_imports(imp: ty.Any) -> ty.Iterable[str]:
        names: ty.Iterable[ty.Optional[str]]
        if isinstance(imp, ast.Import):
            names = (n.name for n in imp.names)
        elif isinstance(imp, ast.ImportFrom):
            names = (imp.module,)
        else:
            names = ()
        return filter(None, names)

    def _extract_ancestors(module: str) -> ty.Iterable[str]:
        parts = module.split(".")
        return (".".join(parts[:i]) for i in range(1, len(parts) + 1))

    imported_modules = itertools.chain.from_iterable(map(_extract_imports, statements))
    all_imported_modules = itertools.chain.from_iterable(map(_extract_ancestors, imported_modules))
    return set(all_imported_modules)


@contextmanager
def clear_and_restore_import_cache(module_name_filter: ty.Callable[[str], ty.Any]) -> ty.Iterator[None]:
    already_imported = [name for name in sys.modules if module_name_filter(name)]
    if already_imported:
        getLogger(__name__).debug(
            "Clearing the following from sys.modules matching %s:\n  %s",
            module_name_filter,
            "\n  ".join(already_imported),
        )
    to_restore = {name: sys.modules.pop(name) for name in already_imported}
    try:
        yield
    finally:
        sys.modules.update(to_restore)


def assert_dev_deps_not_imported(import_statement: str, forbidden_modules: ty.Collection[str]) -> None:
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
    # ensure that we clear the cache of the actually imported modules, lest we get a spurious pass
    # due to the interpreter not evaluating them again!
    imported_modules = module_names_from_import_statement(import_statement)
    will_be_imported = module_name_re(imported_modules).match
    with clear_and_restore_import_cache(lambda name: is_forbidden(name) or will_be_imported(name)):
        exec(import_statement, {}, {})
        mistakenly_imported = [name for name in sys.modules if is_forbidden(name)]
        assert (
            not mistakenly_imported
        ), f"Modules {', '.join(mistakenly_imported)} were imported on execution of {import_statement!r}"
