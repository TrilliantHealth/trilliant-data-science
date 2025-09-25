import re
from enum import Enum
from typing import Dict, List, Optional, Pattern, Tuple, Union

import pandas as pd
import pandera as pa
from pydantic import BaseModel, Extra, StrictFloat, StrictInt

from .dtypes import DType
from .util import EnumList

Numeric = Union[StrictInt, StrictFloat]


class ColumnConstraint(BaseModel, extra=Extra.forbid):
    __dtypes__: Tuple[DType, ...] = ()

    def applies_to(self, dtype: DType) -> bool:
        return dtype in self.__dtypes__

    def pandera_check_expr(self) -> str:
        raise NotImplementedError(
            f"{type(self).__name__} must implement pandera_check producing an expression "
            "equivalent to the expression produced by the `.pandera_check` method "
            "(with pandera aliased to 'pa')"
        )

    def pandera_check(self) -> pa.Check:
        raise NotImplementedError(
            f"{type(self).__name__} must implement pandera_check producing a pandera.Check object "
            "equivalent to the expression produced by the `.pandera_check_expr` method"
        )

    def sqlite_check_expr(self, colname: str) -> str:
        raise NotImplementedError(f"sqlite check constraint not implemented for constraint {self}")

    def required_modules(self) -> List[str]:
        """list of stdlib modules required for constraint checks"""
        return []

    def comment_expr(self) -> Optional[str]:
        return None


class StrConstraint(ColumnConstraint):
    __dtypes__ = (DType.STR,)


class LenConstraint(StrConstraint):
    __operator__: str
    __value_attr__: str

    def pandera_check_expr(self) -> str:
        kwargs_ = ", ".join(f"{k}={v!r}" for k, v in self._pandera_check_kwargs().items())
        return f"pa.{pa.Check.__name__}.{pa.Check.str_length.__name__}({kwargs_})"

    def pandera_check(self) -> pa.Check:
        return pa.Check.str_length(**self._pandera_check_kwargs())

    def _pandera_check_kwargs(self) -> Dict[str, int]:
        kw = "max_value" if "<" in self.__operator__ else "min_value"
        value = getattr(self, self.__value_attr__)
        if "=" not in self.__operator__:
            # non-inclusive bound
            if "<" in self.__operator__:
                value -= 1
            else:
                value += 1
        return {kw: value}

    def sqlite_check_expr(self, colname: str) -> str:
        return f"length({colname}) {self.__operator__} {getattr(self, self.__value_attr__)!r}"

    def comment_expr(self) -> Optional[str]:
        return f"length {self.__operator__} {getattr(self, self.__value_attr__)!r}"


class NumConstraint(ColumnConstraint):
    __dtypes__ = tuple(t for t in DType if t.is_float_type or t.is_int_type)


class OrderConstraint(NumConstraint):
    __operator__: str
    __value_attr__: str

    def applies_to(self, dtype: DType) -> bool:
        value = getattr(self, self.__value_attr__)
        if dtype.is_int_type:
            return isinstance(value, int)
        elif dtype.is_float_type:
            return isinstance(value, float)
        return False

    def pandera_check_expr(self) -> str:
        return f"pa.{pa.Check.__name__}.{self.__value_attr__}({getattr(self, self.__value_attr__)!r})"

    def pandera_check(self) -> pa.Check:
        return getattr(pa.Check, self.__value_attr__)(getattr(self, self.__value_attr__))

    def sqlite_check_expr(self, colname) -> str:
        return f"{colname} {self.__operator__} {getattr(self, self.__value_attr__)!r}"

    def comment_expr(self) -> Optional[str]:
        return f"{self.__operator__} {getattr(self, self.__value_attr__)!r}"


class LessThanOrEqual(OrderConstraint):
    le: Numeric
    __operator__ = "<="
    __value_attr__ = "le"


class GreaterThanOrEqual(OrderConstraint):
    ge: Numeric
    __operator__ = ">="
    __value_attr__ = "ge"


class LessThan(OrderConstraint):
    lt: Numeric
    __operator__ = "<"
    __value_attr__ = "lt"


class GreaterThan(OrderConstraint):
    gt: Numeric
    __operator__ = ">"
    __value_attr__ = "gt"


class EqualModulo(NumConstraint):
    eq: Numeric
    mod: Numeric

    def sqlite_check_expr(self, colname: str) -> str:
        return f"{colname} % {self.mod} = {self.eq}"

    def pandera_check_expr(self) -> str:
        return f"pa.{pa.Check.__name__}(lambda s: (s % {self.mod} == {self.eq}), name={repr(self)!r})"

    def pandera_check(self) -> pa.Check:
        return pa.Check(lambda s: (s % self.mod == self.eq), name=repr(self))

    def comment_expr(self) -> str:
        return f"equals {self.eq!r} modulo {self.mod!r}"


class LenLessThanOrEqual(LenConstraint):
    len_le: StrictInt
    __operator__ = "<="
    __value_attr__ = "len_le"


class LenGreaterThanOrEqual(LenConstraint):
    len_ge: StrictInt
    __operator__ = ">="
    __value_attr__ = "len_ge"


class LenLessThan(LenConstraint):
    len_lt: StrictInt
    __operator__ = "<"
    __value_attr__ = "len_lt"


class LenGreaterThan(LenConstraint):
    len_gt: StrictInt
    __operator__ = ">"
    __value_attr__ = "len_gt"


class StrCase(Enum):
    lower = "lower"
    upper = "upper"


class StrChars(Enum):
    alpha = "alpha"
    alphanumeric = "alnum"
    digit = "digit"
    decimal = "decimal"
    title = "title"


class CaseConstraint(StrConstraint):
    case: StrCase

    def pandera_check_expr(self) -> str:
        method_name = self._pandas_str_method_name()
        check_name = self._check_name()
        return (
            f"pa.{pa.Check.__name__}(lambda s: s.str.{method_name}().fillna(True), name={check_name!r})"
        )

    def pandera_check(self) -> pa.Check:
        method_name = self._pandas_str_method_name()
        check_name = self._check_name()
        return pa.Check(lambda s: getattr(s.str, method_name)().fillna(True), name=check_name)

    def _check_name(self) -> str:
        return f"case={self.case.value}"

    def _pandas_str_method_name(self) -> str:
        return f"is{self.case.value}"

    def sqlite_check_expr(self, colname: str) -> str:
        return f"{colname} = {self.case.value}({colname})"

    def comment_expr(self) -> Optional[str]:
        return self.case.value + "case"


class CharsConstraint(StrConstraint):
    chars: StrChars

    def pandera_check_expr(self) -> str:
        method_name = self._pandas_str_method_name()
        check_name = self._check_name()
        return (
            f"pa.{pa.Check.__name__}(lambda s: s.str.{method_name}().fillna(True), name={check_name!r})"
        )

    def pandera_check(self) -> pa.Check:
        method_name = self._pandas_str_method_name()
        check_name = self._check_name()
        return pa.Check(lambda s: getattr(s.str, method_name)().fillna(True), name=check_name)

    def _check_name(self) -> str:
        return f"chars={self.chars.value}"

    def _pandas_str_method_name(self) -> str:
        return f"is{self.chars.value}"

    def comment_expr(self) -> Optional[str]:
        return self.chars.name + " pattern"


class MatchesRegex(StrConstraint):
    matches: Pattern
    fullmatch: bool = True
    case_sensitive: bool = True

    def pandera_check_expr(self) -> str:
        method_name = "fullmatch" if self.fullmatch else "match"
        check_name = self._check_name()
        return (
            f"pa.{pa.Check.__name__}(lambda s: s.str.{method_name}"
            f"(re.compile({self.matches.pattern!r}), "
            f"case={self.case_sensitive}, na=True), name={check_name!r})"
        )

    def pandera_check(self) -> pa.Check:
        check_name = self._check_name()
        if self.fullmatch:

            def check_fn(s: pd.Series):
                return s.str.fullmatch(
                    re.compile(self.matches.pattern), case=self.case_sensitive, na=True  # type: ignore[arg-type]
                )

        else:

            def check_fn(s: pd.Series):
                return s.str.match(
                    re.compile(self.matches.pattern),  # type: ignore[arg-type]
                    case=self.case_sensitive,
                    na=True,
                )

        # TODO - check above type ignores

        return pa.Check(check_fn, name=check_name)

    def _check_name(self) -> str:
        return f"{type(self).__name__}(fullmatch={self.fullmatch}, case_sensitive={self.case_sensitive})"

    def required_modules(self) -> List[str]:
        return ["re"]

    def comment_expr(self) -> Optional[str]:
        extras = [
            "full match" if self.fullmatch else "prefix match",
            "case sensitive" if self.case_sensitive else "case insensitive",
        ]
        extra = f" ({', '.join(extras)})"
        return f"matches ``{self.matches.pattern}``{extra}"


class EnumConstraint(ColumnConstraint):
    __dtypes__ = (DType.STR, *(t for t in DType if t.is_int_type or t.is_float_type))

    enum: EnumList
    ordered: bool = False

    def pandera_check_expr(self) -> str:
        # pandera doesn't support checking specific categoricals natively, only
        # that a column has a categorical dtype, so we check the values here
        return f"pa.{pa.Check.__name__}.isin({self.enum!r})"

    def pandera_check(self) -> pa.Check:
        return pa.Check.isin(self.enum)

    def sqlite_check_expr(self, colname: str) -> str:
        return f'{colname} IN ({", ".join(map(repr, self.enum))})'

    def applies_to(self, dtype: DType) -> bool:
        if not self.enum:
            return True
        value_type = type(self.enum[0])
        return issubclass(value_type, dtype.python)


AnyColumnConstraint = Union[
    LessThanOrEqual,
    GreaterThanOrEqual,
    LessThan,
    GreaterThan,
    EqualModulo,
    LenLessThanOrEqual,
    LenGreaterThanOrEqual,
    LenLessThan,
    LenGreaterThan,
    EnumConstraint,
    CaseConstraint,
    CharsConstraint,
    MatchesRegex,
]
