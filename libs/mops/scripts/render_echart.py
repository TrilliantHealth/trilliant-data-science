#!/usr/bin/env -S uv run --script
# /// script
# dependencies = [
#   "nicegui",
#   "hjson",
# ]
# ///

import hjson  # type: ignore
from nicegui import ui  # type: ignore


@ui.page("/")
def index():
    echart_option = hjson.loads(open("echart_graph.json").read())

    with ui.element("div").style("width: fit-content; height: 100vh; margin: auto;"):
        ui.echart(echart_option).style("width: 100vw; height: 100vh")


if __name__ in {"__main__", "__mp_main__"}:
    ui.run()
