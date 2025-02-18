import itertools
import random
import typing as ty
from functools import partial

from colors import color, csscolors

pref = "\033["
reset = f"{pref}0m"

_RESERVED_COLORS = [
    "black",
    # Various whitish-looking colors
    "aliceblue",
    "antiquewhite",
    "floralwhite",
    "ghostwhite",
    "ivory",
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
    # These are pretty illegible on a black background
    "darkblue",
    "indigo",
    "mediumblue",
    "navy",
    "purple",
]

_PREFERRED_COLORS = [
    "mediumseagreen",
    "cornflowerblue",
    "gold",
    "salmon",
    "violet",
    "limegreen",
    "dodgerblue",
    "goldenrod",
    "indianred",
    "fuchsia",
    "forestgreen",
    "royalblue",
    "yellow",
    "chocolate",
    "palevioletred",
    "mediumspringgreen",
    "deepskyblue",
    "khaki",
    "red",
    "deeppink",
    "seagreen",
    "cyan",
    "greenyellow",
    "sandybrown",
    "orchid",
    "lightgreen",
    "steelblue",
    "darkgoldenrod",
    "coral",
    "darkorchid",
]


def _start_from(color_list: ty.List[str], index: int) -> ty.List[str]:
    return color_list[index:] + color_list[:index]


def _preferred_randgreen_start() -> ty.List[str]:
    return _start_from(_PREFERRED_COLORS, random.randint(0, 6) * 5)


def _all_colors() -> ty.List[str]:
    forbidden_colors = {csscolors.css_colors[name] for name in _RESERVED_COLORS}
    used_colors = {csscolors.css_colors[name] for name in _PREFERRED_COLORS}
    assert len(used_colors) == len(_PREFERRED_COLORS)  # assert no RGB dupes in the preferred list
    all_colors = list(csscolors.css_colors.items())
    random.shuffle(all_colors)
    return _preferred_randgreen_start() + [
        name
        for name, rgb in all_colors
        if rgb not in used_colors
        and not used_colors.add(rgb)  # type: ignore
        and rgb not in forbidden_colors
    ]


next_color = ty.cast(ty.Callable[[], str], partial(next, itertools.cycle(_all_colors())))


def colorized(fg: str, bg: str = "", style: str = "") -> ty.Callable[[str], str]:
    def colorize(s: str) -> str:
        return color(s, fg=fg, bg=bg, style=style)

    return colorize


def make_colorized_out(
    colorized: ty.Callable[[str], str], *, fmt_str: str = "{}", out: ty.Callable[[str], ty.Any] = print
) -> ty.Callable[[str], None]:
    def _out(s: str) -> None:
        out(colorized(fmt_str.format(s)))

    return _out
