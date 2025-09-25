import datetime
import random
import string
from functools import partial

from .util import Gen

MIN_INT = -(2**32)
MAX_INT = 2**32 - 1
MIN_FLOAT = -float(MIN_INT)
MAX_FLOAT = float(MAX_INT)
MIN_DATE = datetime.date(1970, 1, 1)
MAX_DATE = datetime.date.today()
STR_MAX_LEN = 32


def random_null():
    return None


def random_int(lo: int = MIN_INT, hi: int = MAX_INT) -> int:
    return random.randint(lo, hi)


def random_int_gen(lo: int = MIN_INT, hi: int = MAX_INT) -> Gen[int]:
    return partial(random.randint, lo, hi)


def random_float(lo: float = MIN_FLOAT, hi: float = MAX_FLOAT) -> float:
    return random.uniform(lo, hi)


def random_float_gen(lo: float = MIN_FLOAT, hi: float = MAX_FLOAT) -> Gen[float]:
    return partial(random.uniform, lo, hi)


default_str_len_gen = random_int_gen(0, STR_MAX_LEN)


def random_str(len_gen: Gen[int] = default_str_len_gen, chars: str = string.printable) -> str:
    return "".join(random.choices(chars, k=len_gen()))


def random_str_gen(len_gen: Gen[int] = default_str_len_gen, chars: str = string.printable) -> Gen[str]:
    return partial(random_str, len_gen, chars)


def random_bool(true_rate: float = 0.5) -> bool:
    return random.random() < true_rate


def random_bool_gen(true_rate: float = 0.5) -> Gen[bool]:
    return partial(random_bool, true_rate)


default_date_offset_gen = random_int_gen(0, (MAX_DATE - MIN_DATE).days)


def random_date(
    earliest: datetime.date = MIN_DATE,
    offset: Gen[int] = default_date_offset_gen,
) -> datetime.date:
    return earliest + datetime.timedelta(days=offset())


def random_date_gen(
    earliest: datetime.date,
    offset: Gen[int] = default_date_offset_gen,
) -> Gen[datetime.date]:
    return partial(random_date, earliest, offset)


def random_date_gen_from_range(earliest: datetime.date, latest: datetime.date):
    return random_date_gen(earliest, random_int_gen(0, (latest - earliest).days))


default_datetime_offset_gen = random_float_gen(0.0, (MAX_DATE - MIN_DATE).total_seconds())


def random_datetime(
    earliest: datetime.date = MIN_DATE,
    offset: Gen[float] = default_datetime_offset_gen,
) -> datetime.date:
    return earliest + datetime.timedelta(seconds=offset())


def random_datetime_gen(
    earliest: datetime.datetime,
    offset: Gen[float] = default_datetime_offset_gen,
) -> Gen[datetime.date]:
    return partial(random_datetime, earliest, offset)


def random_datetime_gen_from_range(earliest: datetime.datetime, latest: datetime.datetime):
    return random_datetime_gen(earliest, random_float_gen(0.0, (latest - earliest).total_seconds()))


def random_bytes(len_gen: Gen[int] = default_str_len_gen):
    return bytes(random.randint(0, 255) for _ in range(len_gen()))


def random_bytearray(len_gen: Gen[int] = default_str_len_gen):
    return bytearray(random.randint(0, 255) for _ in range(len_gen()))
