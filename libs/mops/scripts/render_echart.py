#!/usr/bin/env -S uv run --script
# /// script
# dependencies = [
#   "nicegui",
#   "hjson",
# ]
# ///

import hjson  # type: ignore
from nicegui import ui  # type: ignore


def render(echart_file: str) -> None:
    @ui.refreshable
    def render_echart(echart_file: str) -> None:
        echart_option = hjson.loads(open(echart_file).read())
        ui.echart(echart_option).style("width: 100vw; height: 100vh")

    render_echart(echart_file)

    ui.button("Refresh", on_click=render_echart.refresh).style(
        "position: absolute; top: 10px; right: 10px"
    )


with ui.element("div").style("width: fit-content; height: 100vh; margin: auto;"):
    render("echart_graph.json")


if __name__ in {"__main__", "__mp_main__"}:
    ui.run()
