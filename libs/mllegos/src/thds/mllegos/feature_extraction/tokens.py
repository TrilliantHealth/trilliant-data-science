import dataclasses
import functools
from typing import Dict, Generic, Hashable, Mapping, Optional, TypeVar

import pandas as pd

from ..util.aliases import Normalizer

K = TypeVar("K", bound=Hashable)
T = TypeVar("T")


@dataclasses.dataclass(frozen=False)
class TokenLookup(Generic[K, T]):
    encoding: Mapping[K, T]
    unknown_value: T
    null_value: Optional[T]
    normalizer: Optional[Normalizer[K]] = None

    def __post_init__(self):
        self._cache: Dict[Optional[K], T] = {}
        self._lookup = (
            functools.partial(
                lookup_token,
                self.encoding,
                self.unknown_value,
                self.null_value,
            )
            if self.normalizer is None
            else functools.partial(
                lookup_token_normalized,
                self.encoding,
                self.unknown_value,
                self.null_value,
                self.normalizer,
            )
        )

    def __call__(self, value: Optional[K]) -> T:
        if (code := self._cache.get(value)) is not None:
            return code
        code = self._cache[value] = self._lookup(value)
        return code


def lookup_token(
    encoding: Mapping[K, T],
    unknown_value: T,
    null_value: Optional[T],
    value: Optional[K],
) -> T:
    """Encode values of type `K` into values of type `T` using a lookup, e.g. to map string tokens to
    integer indices. Accomodates missing values (including those recognized as null by pandas for
    convenience).

    Intended for use with functools.partial binding of the leading args.
    """
    if pd.isna(value):  # type: ignore[arg-type]
        return unknown_value if null_value is None else null_value
    return encoding.get(value, unknown_value)


def lookup_token_normalized(
    encoding: Mapping[K, T],
    unknown_value: T,
    null_value: Optional[T],
    normalizer: Normalizer[K],
    value: Optional[K],
) -> T:
    """Encode values of type `K` into values of type `T` using a lookup, e.g. to map string tokens to
    integer indices. Accomodates missing values (including those recognized as null by pandas for
    convenience) and normalizes values before lookup using the `normalizer` function.

    Intended for use with functools.partial binding of the leading args.
    """
    if pd.isna(value):  # type: ignore[arg-type]
        return unknown_value if null_value is None else null_value
    if normalizer is not None:
        normalized = normalizer(value)
        if normalized is None:
            # don't know what to do with this value
            return unknown_value
        return encoding.get(normalized, unknown_value)
    return encoding.get(value, unknown_value)
