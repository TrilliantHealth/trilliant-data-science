from typing import Literal, NewType

import attr

Enum = Literal["foo", "bar", "baz"]
NT = NewType("NT", str)
NewNewType = NewType("NewNewType", NT)


@attr.define(hash=True, order=True)
class Record:
    x: NT
    y: Enum
