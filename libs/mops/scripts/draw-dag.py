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


def create_dag(
    log_entries: ty.List[run_summary.LogEntry], xform_node_name: ty.Callable[[str], str]
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

        memo_uri = memo_uri.replace(log_entry.get("runner_prefix", ""), "")
        memo_uri = memo_uri.replace(log_entry.get("pipeline_id", ""), "")
        memo_uri = memo_uri.replace("@" + log_entry.get("function_logic_key", ""), "")
        memo_uri = memo_uri.strip("/")
        memo_uri = memo_uri[:-40]  # get rid of the numeric bits of the hash

        if input_uris := log_entry.get("uris_in_args_kwargs"):
            for uri in input_uris:
                sink_functions_by_uri[uri].add(memo_uri)
        else:
            g.add_node(memo_uri)

        if output_uris := log_entry.get("uris_in_rvalue"):
            for uri in output_uris:
                source_functions_by_uri[uri].add(memo_uri)
        else:
            g.add_node(memo_uri)

    # now we can create the graph:
    for uri, sink_functions in sink_functions_by_uri.items():
        for source_function in source_functions_by_uri[uri]:
            for sink_function in sink_functions:
                g.add_edge(source_function, sink_function)

    for node in g.nodes():
        g.nodes[node]["label"] = xform_node_name(node)

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


def xform_node_name(name: str) -> str:
    if name.startswith("thds."):
        name = name[5:]
    if "--" not in name:
        return name
    module, function_and_args = name.split("--")
    function_name, args = function_and_args.split("/", 1)
    return f"{module}\n{function_name}({args})"


def is_uninteresting_regexes(regexes: ty.List[str]) -> ty.Callable[[Node], bool]:
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
) -> None:
    dag = create_dag(load_all_log_entries(auto_find_run_directory(run_dir)), xform_node_name)
    if remove_nodes:
        dag = remove_unininteresting_nodes(
            is_uninteresting_regexes(remove_nodes),
            dag,
        )
    try:
        dagviz_render(dag) or graphviz_render(dag, subgraph_config) or pyvis_render(dag)
    except Exception:
        print(nx.find_cycle(dag))


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
    )


if __name__ == "__main__":
    main()
