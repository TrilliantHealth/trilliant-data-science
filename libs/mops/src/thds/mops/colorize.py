import random
import typing as ty

from colors import color, csscolors

pref = "\033["
reset = f"{pref}0m"

_RESERVED_COLORS = [
    "antiquewhite",
    "black",
    "ghostwhite",
    "white",
    "whitesmoke",
    "snow",
    "seashell",
    "mintcream",
    "honeydew",
    "azure",
    "beige",
    "cornsilk",
    "floralwhite",
]

_RANDOM_COLORS = [c for c in csscolors.css_colors.keys() if c not in _RESERVED_COLORS]
random.shuffle(_RANDOM_COLORS)

_NEXT_COLOR = -1


def next_color() -> str:
    global _NEXT_COLOR
    _NEXT_COLOR += 1
    return _RANDOM_COLORS[_NEXT_COLOR % len(_RANDOM_COLORS)]


def colorized(fg: str, bg: str = "", style: str = "") -> ty.Callable[[str], str]:
    def colorize(s: str) -> str:
        return color(s, fg=fg, bg=bg, style=style)

    return colorize


def make_colorized_out(colorized, prefix: str = "", out=print):

    if prefix[-1] != " ":
        prefix += " "

    def _out(s: str):
        out(colorized(prefix + s))

    return _out
