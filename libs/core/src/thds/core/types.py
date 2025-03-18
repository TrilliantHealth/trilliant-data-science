import os
import typing as ty

StrOrPath = ty.Union[
    str, os.PathLike
]  # DEPRECATED - please be explicit about this bc it isn't much extra typing
