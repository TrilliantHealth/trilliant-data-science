"""Import this module by its name, so that references to things within it are qualified by
the word 'generators', e.g. generators.sender()
"""
import contextlib
import typing as ty

T = ty.TypeVar("T")
R = ty.TypeVar("R")
GEN = ty.TypeVar("GEN", bound=ty.Generator)


class return_wrapper(contextlib.AbstractContextManager, ty.Generic[GEN, R]):
    """Allows you to wrap a generator that accepts and/or yields values,
    but this will prime the generator and also close it at the end and fetch
    its return value.

    This will be somewhat easier in 3.13 with the new `gen.close()` behavior.
    https://discuss.python.org/t/let-generator-close-return-stopiteration-value/24786
    """

    def __init__(self, gen: GEN):
        self.gen = gen

    def __enter__(self) -> GEN:
        next(self.gen)  # prime the generator
        return self.gen

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is not None:
            # TODO confirm that this is the correct behavior
            self.gen.throw(exc_type, exc_value, traceback)

        try:
            self.gen.throw(GeneratorExit)
            # equivalent to gen.close() but also gives us StopIteration.value
        except StopIteration as e:
            self._return_value = e.value

    @property
    def return_value(self) -> R:
        """Only available after the context manager has exited."""
        return self._return_value


def iterator_sender(gen: ty.Generator[ty.Any, T, R], iterator: ty.Iterable[T]) -> R:
    """This encapsulates the send/close behavior we want in general. See
    https://discuss.python.org/t/let-generator-close-return-stopiteration-value/24786
    for how a simple `gen.close()` will do this in 3.13.
    """

    gen_wrapper: return_wrapper[ty.Generator, R] = return_wrapper(gen)  # type: ignore[arg-type]
    with gen_wrapper:
        for i in iterator:
            gen.send(i)

    return gen_wrapper.return_value
