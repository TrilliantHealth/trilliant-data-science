import typing as ty
from functools import partial

A = ty.TypeVar("A")
B = ty.TypeVar("B")
C = ty.TypeVar("C")


def _compose(f2: ty.Callable[[B], C], f1: ty.Callable[[A], B], arg: A) -> C:
    return f2(f1(arg))


def compose(f2: ty.Callable[[B], C], f1: ty.Callable[[A], B]) -> ty.Callable[[A], C]:
    """right-to-left"""
    return partial(_compose, f2, f1)


def pipe(f1: ty.Callable[[A], B], f2: ty.Callable[[B], C]) -> ty.Callable[[A], C]:
    """left-to-right"""
    return partial(_compose, f2, f1)
