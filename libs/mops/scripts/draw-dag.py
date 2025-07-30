#!/usr/bin/env -S uv run --script
# /// script
# dependencies = [
#   "hjson",
#   "networkx",
#   "thds.mops @ file:///${PROJECT_ROOT}/",
# ]
# ///
import json
import re
import sys
import typing as ty
from collections import defaultdict
from pathlib import Path
from typing import Any

import hjson  # type: ignore
import networkx as nx  # type: ignore

from thds.mops.pure.tools.summarize import run_summary
from thds.mops.pure.tools.summarize.cli import auto_find_run_directory


def load_all_log_entries(log_dir: Path) -> ty.List[run_summary.LogEntry]:
    log_entries = []
    for log_file in log_dir.glob("*.json"):
        with log_file.open() as f:
            log_data: run_summary.LogEntry = json.load(f)
            log_entries.append(log_data)
    return log_entries


def _xform_memo_uri_into_node_id(
    memo_uri: str, runner_prefix: str, pipeline_id: str, function_logic_key: str
) -> str:
    """
    Transform a memo URI into a node ID by removing the runner prefix, pipeline ID,
    and function logic key, and stripping trailing numeric bits of the hash.
    """
    memo_uri = memo_uri.replace(runner_prefix, "")
    memo_uri = memo_uri.replace(pipeline_id, "")
    memo_uri = memo_uri.replace("@" + function_logic_key, "")
    memo_uri = memo_uri.strip("/")
    return memo_uri[:-40]  # get rid of the numeric bits of the hash


NodeIdTransformer = ty.Callable[[str, str, str, str], str]


class SafeNodeIdTransformer:
    """
    Transforms and sanitizes node IDs while ensuring no collisions are introduced.

    This class applies a series of substring replacements to node IDs and validates
    that the transformation process does not cause two different original IDs to
    be mapped to the same sanitized ID, which would break the graph structure.
    """

    def __init__(self, substring_replacements: ty.Mapping[str, str]):
        """
        Initializes the transformer with a set of replacement rules.

        Args:
            substring_replacements: A dictionary where keys are the substrings
                to find and values are the strings to replace them with.
        """
        self.replacements = substring_replacements
        self.transformed_ids: dict[str, set[str]] = defaultdict(set)

    def __call__(
        self,
        memo_uri: str,
        runner_prefix: str,
        pipeline_id: str,
        function_logic_key: str,
    ) -> str:
        """
        Applies the full transformation and sanitization process to a single ID.

        Args:
            memo_uri: The original, unique identifier for the node.
            runner_prefix: The prefix of the runner to be removed.
            pipeline_id: The ID of the pipeline to be removed.
            function_logic_key: The function logic key to be removed.

        Returns:
            The sanitized and shortened node ID.

        Raises:
            ValueError: If a new ID collision is detected.
        """
        # 1. Perform the original, coarse transformation
        base_id = _xform_memo_uri_into_node_id(memo_uri, runner_prefix, pipeline_id, function_logic_key)

        # 2. Apply the configured substring replacements for sanitization
        final_id = base_id
        for from_str, to_str in self.replacements.items():
            final_id = final_id.replace(from_str, to_str)

        # 3. Check for collisions
        # We track which original URIs map to each final ID.
        self.transformed_ids[final_id].add(memo_uri)

        if len(self.transformed_ids[final_id]) > 1:
            collided_uris = self.transformed_ids[final_id]
            raise ValueError(
                f"ID collision detected! The new ID '{final_id}' was generated "
                f"from multiple different source URIs: {collided_uris}"
            )

        return final_id


def create_dag(
    log_entries: ty.List[run_summary.LogEntry],
    xform_node_name: ty.Callable[[str], str],
    xform_node_id: NodeIdTransformer,
) -> nx.DiGraph:
    g = nx.DiGraph()
    # each log entry file is a node - we use the function_name as the node name.  the
    # links between the nodes are the uris_in_args_kwargs and uris_in_rvalue -
    # essentially, a node depends _on_ whatever functions produced (have uris_in_rvalue) the uris found in that
    # list.

    # so first we need to create a lookup for each URI - which function_name it comes _from_, and then which
    # function_names it goes to:
    source_functions_by_uri = defaultdict(set)
    sink_functions_by_uri = defaultdict(set)

    for log_entry in log_entries:
        memo_uri = log_entry.get("memo_uri")
        if not memo_uri:
            continue

        node_id = xform_node_id(
            memo_uri,
            log_entry.get("runner_prefix", ""),
            log_entry.get("pipeline_id", ""),
            log_entry.get("function_logic_key", ""),
        )

        if input_uris := log_entry.get("uris_in_args_kwargs"):
            for uri in input_uris:
                sink_functions_by_uri[uri].add(node_id)
        else:
            g.add_node(node_id)

        if output_uris := log_entry.get("uris_in_rvalue"):
            for uri in output_uris:
                source_functions_by_uri[uri].add(node_id)
        else:
            g.add_node(node_id)

    # now we can create the graph:
    for uri, sink_functions in sink_functions_by_uri.items():
        for source_function in source_functions_by_uri[uri]:
            for sink_function in sink_functions:
                g.add_edge(source_function, sink_function)

    for node_id in g.nodes():
        g.nodes[node_id]["label"] = xform_node_name(node_id)

    return g


def add_edges(edges_to_add: ty.Collection[ty.Sequence[str]], g: nx.DiGraph) -> nx.DiGraph:
    for edge in edges_to_add:
        if len(edge) != 2:
            raise ValueError(f"Invalid edge: {edge}. Must be a pair of node names.")
        g.add_edge(edge[0], edge[1])
    return g


class SubgraphConfig(ty.TypedDict):
    name: str
    patterns: list[str]
    parent: ty.Optional[str]
    fillcolor: str


def apply_subgraph_config(subgraph_config: ty.Mapping[str, SubgraphConfig], g: nx.DiGraph) -> nx.DiGraph:
    for node in g.nodes():
        for sg in subgraph_config.values():
            # first match wins
            if any(re.match(p, node) for p in sg["patterns"]):
                g.nodes[node]["subgraph"] = sg["name"]
                g.nodes[node]["fillcolor"] = sg["fillcolor"]
                break
    return g


class Coordinates(ty.TypedDict):
    x: float
    y: float


def apply_fixed_coordinates(
    fixed_coordinates: ty.Mapping[str, Coordinates], g: nx.DiGraph
) -> nx.DiGraph:
    try:
        for node, coords in fixed_coordinates.items():
            g.nodes[node]["x"] = coords["x"]
            g.nodes[node]["y"] = coords["y"]
            g.nodes[node]["fixed"] = True
    except KeyError:
        print(list(g.nodes.keys()))
        raise

    return g


Node = ty.Any  # Placeholder for the actual node type


def remove_unininteresting_nodes(
    is_uninteresting: ty.Callable[[Node], Node], dag: nx.DiGraph
) -> nx.DiGraph:
    def bypass_node(G, node_to_remove):
        predecessors = list(G.predecessors(node_to_remove))
        successors = list(G.successors(node_to_remove))

        # Add edges from all predecessors to all successors
        for pred in predecessors:
            for succ in successors:
                G.add_edge(pred, succ)

        # Remove the node
        G.remove_node(node_to_remove)

    for node in list(dag.nodes):
        if is_uninteresting(node):
            bypass_node(dag, node)

    return dag


def pyvis_render(dag: nx.DiGraph) -> bool:
    try:
        from pyvis.network import Network  # type: ignore[import]

        nt = Network("1000px", "1000px")
        nt.from_nx(dag)
        nt.show("dag.html", notebook=False)
        return True
    except ImportError:
        return False


def dagviz_render(dag: nx.DiGraph) -> bool:
    try:
        from dagviz import render  # type: ignore[import]

        render(dag, filename="dagviz_dag.svg", format="svg")
        return True
    except ImportError:
        return False


def graphviz_render(
    dag: nx.DiGraph,
    subgraph_map: ty.Mapping[str, ty.Mapping[str, ty.Any]],
) -> bool:
    try:
        import pygraphviz as pgv  # type: ignore[import]

        def relax_subgraph_fan_in_constraints(graph: pgv.AGraph, fan_in_threshold: int = 3) -> None:
            """
            Inspects subgraphs within a pygraphviz AGraph and sets constraint=false
            on edges pointing to nodes that have a high fan-in *from within that
            same subgraph*.

            Modifies the graph in place.

            Args:
                graph: The pygraphviz.AGraph object to modify.
                fan_in_threshold: The minimum number of incoming edges *from within
                                  the same subgraph* for a node's incoming internal
                                  edges to have their constraint relaxed. Defaults to 3.
                                  The constraint is relaxed if fan-in > threshold.
            """
            if not isinstance(graph, pgv.AGraph):
                raise TypeError("Input 'graph' must be a pygraphviz.AGraph object.")
            if not isinstance(fan_in_threshold, int) or fan_in_threshold < 0:
                raise ValueError("'fan_in_threshold' must be a non-negative integer.")

            # Cache all edges from the main graph once to avoid redundant calls
            # Store as (str(u), str(v)) tuples for reliable lookup
            all_graph_edges = [(str(u), str(v)) for u, v in graph.edges_iter()]

            # Iterate through each subgraph defined directly within the main graph
            for subgraph in graph.subgraphs_iter():
                subgraph_node_names: set[str] = {str(n) for n in subgraph.nodes_iter()}
                if not subgraph_node_names:
                    continue  # Skip empty subgraphs

                # --- Calculate internal in-degree for nodes within this subgraph ---
                # Count how many edges *from within this subgraph* point to each node
                # also *within this subgraph*.
                internal_in_degree: dict[str, int] = defaultdict(int)
                # Store the actual (source, target) names of these internal edges
                internal_incoming_edges: dict[str, set[tuple[str, str]]] = defaultdict(set)

                for u_name, v_name in all_graph_edges:
                    # Check if BOTH source and target are in the current subgraph
                    if u_name in subgraph_node_names and v_name in subgraph_node_names:
                        internal_in_degree[v_name] += 1
                        internal_incoming_edges[v_name].add((u_name, v_name))

                # --- Identify target nodes exceeding the threshold ---
                high_fan_in_nodes = {
                    node_name
                    for node_name, degree in internal_in_degree.items()
                    if degree > fan_in_threshold
                }

                # --- Modify constraints for the qualifying incoming edges ---
                if not high_fan_in_nodes:
                    continue  # No nodes met the criteria in this subgraph

                print(
                    f"Subgraph '{subgraph.name}': Found high fan-in nodes: {high_fan_in_nodes}",
                    file=sys.stderr,
                )

                for target_node_name in high_fan_in_nodes:
                    # Iterate through the *internal* edges we previously identified
                    for u_name, v_name in internal_incoming_edges[target_node_name]:
                        try:
                            # Get the edge object from the main graph to modify its attributes
                            # Ensure we use the string names for lookup
                            edge = graph.get_edge(u_name, v_name)
                            edge.attr["constraint"] = "false"
                            # print(f"  Relaxed constraint for edge: {u_name} -> {v_name}", file=sys.stderr)
                        except KeyError:
                            # This might happen if the edge exists conceptually (e.g., added
                            # to subgraph but not main graph? Unlikely but possible)
                            # or if node name string conversion had issues.
                            print(
                                f"Warning: Could not find edge ({u_name} -> {v_name})"
                                " in the main graph to modify constraint.",
                                file=sys.stderr,
                            )
                        except Exception as e:
                            # Catch other potential pygraphviz issues
                            print(
                                f"Warning: Error setting constraint for edge ({u_name} -> {v_name}): {e}",
                                file=sys.stderr,
                            )

        A = nx.nx_agraph.to_agraph(dag)
        A.node_attr.update(
            {
                "width": "1.5",  # Set width constraint
                "height": "0.8",  # Set height
                "shape": "box",  # Rectangular boxes work better with wrapped text
                "fontsize": "16",
                "margin": "0.1,0.1",  # Reduce margins (inches)
                "style": "filled",
                "fillcolor": "white",
                "rank": "sink",
            }
        )

        A.graph_attr.update(
            {
                "clusterrank": "local",  # Cluster subgraphs
                "rankdir": "LR",
                "nodesep": "0.3",  # Reduce horizontal spacing
                # "overlap": "false",
                "splines": "ortho",  # Try orthogonal lines
                "newrank": "true",  # this really seems to help a lot if you also add some constraint=false on some nodes.
                # "pack": "true",  # Pack subgraphs together
                # "packmode": "clust",
                # "size": "10,20!",
            }
        )
        # Alternative: A.graph_attr['splines'] = 'polyline'

        created_subgraphs: dict[str, ty.Any] = dict()
        for name, config in subgraph_map.items():
            print("adding subgraph", name)
            parent = config.get("parent") or ""
            if parent:
                parent_sg = created_subgraphs[parent]
            else:
                parent_sg = A
            sg = parent_sg.add_subgraph(
                [n for n in dag.nodes if any(re.match(p, n) for p in config["patterns"])],
                name=f"cluster_{name}",
                label=name,
                fillcolor=config["fillcolor"],
                style="filled",
            )
            created_subgraphs[name] = sg

        # Relax constraints for nodes with high fan-in
        relax_subgraph_fan_in_constraints(A)

        dot = dict(prog="dot", args="-Gconcentrate=true -Gmaxiter=10000")
        # neato = dict(prog="neato", args="-Gmode=KK -Gdimen=2 -Grepulsiveforce=2.0")
        # circo = dict(prog="circo", args="-Gmaxiter=10000 -Gdimen=3 -Grepulsiveforce=2.0")
        A.draw(
            "graphviz_dag_with_subgraphs.svg",
            format="svg",
            **dot,
        )
        A.write("graphviz_dag_with_subgraphs.dot")
        return True
    except ImportError:
        return False


def nx_to_echarts_graph(
    graph: nx.DiGraph,
    *,  # Enforces keyword-only arguments after this
    default_symbol_size: int = 15,
    tooltip_formatter: str = "",
    force_config: ty.Optional[dict[str, Any]] = None,
    default_category_name: str = "Other",
    default_category_color: str = "#AdAdAd",
) -> dict[str, ty.Any]:
    """
    Converts a NetworkX DiGraph into an ECharts graph option dictionary,
    dynamically discovering categories from node attributes.

    Args:
        graph: The input NetworkX DiGraph. Nodes are expected to potentially
               have 'subgraph' (str), 'fillcolor' (str), 'label' (str),
               'x', 'y', and 'fixed' attributes.
        default_symbol_size: The default size for graph nodes.
        tooltip_formatter: A JavaScript function string for custom tooltips.
                           An empty string uses the ECharts default formatter.
        force_config: Optional dictionary for ECharts 'force' layout settings
                      (e.g., {"repulsion": 180, "edgeLength": 80}).

    Returns:
        A dictionary representing the ECharts option object suitable for
        JSON serialization or direct use in a JS environment.
    """

    echarts_nodes: list[dict[str, Any]] = []
    echarts_links: list[dict[str, str]] = []
    discovered_categories: dict[str, dict[str, Any]] = {}  # name -> {index: int, color: str}
    category_name_list: list[str] = []
    next_category_index = 0

    # --- Discover Categories and Prepare Nodes ---
    has_default_category = False
    for node_id, attrs in graph.nodes(data=True):
        subgraph_name = attrs.get("subgraph")
        node_category_index: int

        if subgraph_name:
            if subgraph_name not in discovered_categories:
                # First time seeing this subgraph, assign index and color
                fill_color = attrs.get("fillcolor", default_category_color)
                discovered_categories[subgraph_name] = {
                    "index": next_category_index,
                    "color": fill_color,
                }
                category_name_list.append(subgraph_name)
                node_category_index = next_category_index
                next_category_index += 1
            else:
                # Existing subgraph
                node_category_index = discovered_categories[subgraph_name]["index"]
        else:
            # Node belongs to the default category
            if not has_default_category:
                # Create the default category if it doesn't exist yet
                discovered_categories[default_category_name] = {
                    "index": next_category_index,
                    "color": default_category_color,
                }
                category_name_list.append(default_category_name)
                node_category_index = next_category_index
                next_category_index += 1
                has_default_category = True
            else:
                # Default category already exists
                node_category_index = discovered_categories[default_category_name]["index"]

        # --- Create Echarts Node Entry ---
        node_entry: dict[str, Any] = {
            "id": node_id,
            "name": attrs.get("label", node_id),  # Use label if present, else ID
            "category": node_category_index,
            "symbolSize": attrs.get("symbolSize", default_symbol_size),
        }
        # Add optional positional hints or fixed status
        if "x" in attrs:
            node_entry["x"] = attrs["x"]
        if "y" in attrs:
            node_entry["y"] = attrs["y"]
        if attrs.get("fixed", False):  # Check if 'fixed' attribute is True
            node_entry["fixed"] = True

        echarts_nodes.append(node_entry)

    # --- Build Echarts Categories list ---
    echarts_categories: list[dict[str, Any]] = [{} for _ in range(next_category_index)]
    for name, cat_data in discovered_categories.items():
        echarts_categories[cat_data["index"]] = {"name": name, "itemStyle": {"color": cat_data["color"]}}

    # --- Process Edges ---
    for u, v in graph.edges():
        # Assume all nodes u, v exist in the graph and were processed.
        # If graph integrity is not guaranteed, add checks here.
        echarts_links.append({"source": u, "target": v})

    # --- Assemble ECharts Option Structure ---
    tooltip_config: dict[str, Any] = {
        "enterable": True,  # Important for clickable links
        "triggerOn": "mousemove|click",
        "leaveDelay": 100,
    }
    # Only set formatter if it's not empty, otherwise let ECharts use default
    if tooltip_formatter:
        tooltip_config["formatter"] = tooltip_formatter

    # Default force layout if none provided
    final_force_config = force_config or {
        "repulsion": 300,
        # "edgeLength": 40,
        "gravity": 0.05,
        "layoutAnimation": False,
        "friction": 1.5,
    }

    echarts_option: dict[str, Any] = {
        # "tooltip": tooltip_config,
        "legend": [{"data": category_name_list}],  # Use discovered names
        "series": [
            {
                "type": "graph",
                "layout": "force",
                "animation": True,
                "roam": True,
                "draggable": True,
                "label": {
                    "show": False,
                    "position": "bottom",  # Position of the label
                    "formatter": "{b}",  # Use node name ('label' or ID)
                },
                "edgeSymbol": ["none", "arrow"],
                "edgeSymbolSize": 7,
                "focusNodeAdjacency": True,
                "categories": echarts_categories,  # Use discovered categories
                "data": echarts_nodes,
                "links": echarts_links,
                "force": final_force_config,
                "itemStyle": {"borderColor": "#0A1E33", "borderWidth": 1.0},
            }
        ],
    }

    return echarts_option


def save_echart_json(echart_option: dict) -> None:
    with open("echart_graph.json", "w") as f:
        json.dump(echart_option, f, indent=4)


def xform_node_name(name: str) -> str:
    if name.startswith("thds."):
        name = name[5:]
    if "--" not in name:
        return name

    # if we have a /calls-[^/]+/ segment, we remove that:
    if "/calls-" in name:
        name = re.sub(r"/calls-[^/]+/", "/", name)
    name_pieces = name.split("--")
    if len(name_pieces) != 2:
        raise ValueError(f"Invalid node name format: {name}. Expected 'module--function/args'.")
    module, function_and_args = name_pieces[0], name_pieces[1]
    function_name, args = function_and_args.split("/", 1)
    return f"{module}\n{function_name}({args})"


def is_uninteresting_regexes(regexes: list[str]) -> ty.Callable[[Node], bool]:
    def is_uninteresting(node: Node) -> bool:
        for regex in regexes:
            if re.match(regex, node):
                return True
        return False

    return is_uninteresting


def create_and_visualize_dag(
    run_dir: Path,
    remove_nodes: list[str],
    subgraph_config: dict,
    fixed_coordinates: ty.Mapping[str, Coordinates],
    edges_to_add: ty.Collection[ty.Sequence[str]],
    node_id_replacements: ty.Mapping[str, str],
) -> None:
    dag = create_dag(
        load_all_log_entries(auto_find_run_directory(run_dir)),
        xform_node_name,
        SafeNodeIdTransformer(node_id_replacements),
    )
    if remove_nodes:
        dag = remove_unininteresting_nodes(
            is_uninteresting_regexes(remove_nodes),
            dag,
        )
    dag = add_edges(edges_to_add, dag)
    dag = apply_subgraph_config(subgraph_config, dag)
    if fixed_coordinates:
        dag = apply_fixed_coordinates(fixed_coordinates, dag)
    try:
        save_echart_json(nx_to_echarts_graph(dag))
        next(
            filter(
                None,
                (
                    dagviz_render(dag),
                    graphviz_render(dag, subgraph_config),
                    pyvis_render(dag),
                ),
            )
        )
    except Exception as e:
        try:
            print(nx.find_cycle(dag))
        except Exception:
            print(e)


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Render a DAG from log files.")
    parser.add_argument(
        "run_dir",
        type=Path,
        default=auto_find_run_directory(),
        help="Directory containing a .mops/summary directory with log files.",
    )
    parser.add_argument("--hjson-config", type=Path, help="HJSON file with various pieces of config.")
    args = parser.parse_args()
    config = hjson.load(open(args.hjson_config)) if args.hjson_config else {}

    create_and_visualize_dag(
        args.run_dir,
        config.get("remove_nodes") or list(),
        {sg["name"]: sg for sg in config.get("subgraphs") or list()},
        config.get("fixed") or dict(),
        config.get("add_edges") or list(),
        config.get("node_id_renaming", dict()).get("substring_replacements", dict()),
    )


if __name__ == "__main__":
    main()
