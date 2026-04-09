from itertools import cycle

"""
DEFAULT_PALETTE, create_color_map, and js_format_color are all taken from the
UD labels app. I'm thinking we need a central place for viz helper functions.
"""

DEFAULT_PALETTE = [
    "#0173b2",
    "#de8f05",
    "#029e73",
    "#d55e00",
    "#cc78bc",
    "#ca9161",
    "#fbafe4",
    "#949494",
    "#ece133",
    "#56b4e9",
]


def create_color_map(
    categories,
    palette=DEFAULT_PALETTE,
):
    return {
        cat: color for cat, color in zip(categories, cycle(palette() if callable(palette) else palette))
    }


def js_format_color(color_map, idx_or_field: int | str) -> str:
    return """
            function (params) {{
                colors = {};
                return colors[params.value{}];
            }}
            """.format(
        color_map, f"[{idx_or_field}]" if isinstance(idx_or_field, int) else f".{idx_or_field}"
    )
