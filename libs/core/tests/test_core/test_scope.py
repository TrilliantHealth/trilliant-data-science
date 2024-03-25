import contextlib as cl

from thds.core import scope


def test_scopes_exit_when_appropriate():
    enter = list()
    exit = list()

    @cl.contextmanager
    def on_exit(**kwargs):
        enter.append(kwargs)
        yield
        exit.append(kwargs)

    @scope.bound
    def inner():
        scope.enter(on_exit(inner=1))

    @scope.bound
    def outer():
        scope.enter(on_exit(outer_1=3))
        inner()
        scope.enter(on_exit(outer_2=2))

    outer()

    # entrance order is fairly obvious
    assert enter == [
        dict(outer_1=3),
        dict(inner=1),
        dict(outer_2=2),
    ]
    # exit order is:
    assert exit == [
        dict(inner=1),  # exit the inner scope
        dict(outer_2=2),  # LIFO outer
        dict(outer_1=3),  # LIFO outer
    ]


def test_scopes_dont_have_to_be_at_every_function():
    enter = list()
    exit = list()

    @cl.contextmanager
    def on_exit(**kwargs):
        enter.append(kwargs)
        yield
        exit.append(kwargs)

    # no boundary here! the entered CMs will exit at the `outer` boundary
    def inner():
        scope.enter(on_exit(inner=1))

    @scope.bound
    def outer():
        scope.enter(on_exit(outer_1=3))
        inner()
        scope.enter(on_exit(outer_2=2))

    outer()

    # entrance order is fairly obvious
    assert enter == [
        dict(outer_1=3),
        dict(inner=1),
        dict(outer_2=2),
    ]
    # exit order is:
    assert exit == [
        dict(outer_2=2),  # LIFO outer
        dict(inner=1),  # exit the inner scope at the outer boundary
        dict(outer_1=3),  # LIFO outer
    ]


# TODO someday build better detection into core.scope
# so that we can tell whether you're reloading a scope, or
# if this is unintentional interference between modules.
def test_recreating_scopes_gives_warning(caplog):
    scope.Scope("peter")
    assert not caplog.records
    scope.Scope("peter")
    assert caplog.records[0].levelname == "WARNING"
    assert "already exists!" in caplog.records[0].message


def test_there_is_a_default_scope_that_doesnt_exit_until_program_ends():
    vals = list()

    @cl.contextmanager
    def val_ctxt(val):
        vals.append(val)
        yield
        vals.pop(-1)

    scope.enter(val_ctxt(1))
    scope.enter(val_ctxt(2))

    assert vals == [1, 2]
