from thds.termtool import colorize


def test_colors(capsys):
    with capsys.disabled():
        print("")
        for color in colorize._all_colors()[:35]:
            print(colorize.colorized(color, style="bold")(color))
