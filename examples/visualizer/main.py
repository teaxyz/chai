import argparse
import cProfile
import pstats
from collections import defaultdict
from os import getenv
from pstats import SortKey
import sys

import psycopg2
import rustworkx as rx
from rustworkx.visualization import graphviz_draw
from tabulate import tabulate

CHAI_DATABASE_URL = getenv("CHAI_DATABASE_URL")


class Graph(rx.PyDiGraph):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.node_index_map: dict[str, int] = {}

    def safely_add_node(self, node: str) -> int:
        if node not in self.node_index_map:
            index = super().add_node(node)
            self.node_index_map[node] = index
            return index
        return self.node_index_map[node]

    def safely_add_nodes(self, nodes: list[str]) -> list[int]:
        return [self.safely_add_node(node) for node in nodes]


class DB:
    """Prepares the sql statements and connects to the database"""

    def __init__(self):
        self.connect()
        self.cursor.execute(
            "PREPARE select_id AS SELECT id FROM packages WHERE name = $1"
        )
        self.cursor.execute(
            "PREPARE select_name AS SELECT name FROM packages WHERE id = $1"
        )
        self.cursor.execute(
            "PREPARE select_deps AS \
            SELECT DISTINCT p.id, d.dependency_id FROM packages p \
            JOIN versions v ON p.id = v.package_id \
            JOIN dependencies d ON v.id = d.version_id \
            WHERE p.id = ANY($1)"
        )

    def connect(self) -> None:
        self.conn = psycopg2.connect(CHAI_DATABASE_URL)
        self.cursor = self.conn.cursor()

    def select_id(self, package: str) -> int:
        self.cursor.execute("EXECUTE select_id (%s)", (package,))
        return self.cursor.fetchone()[0]

    def select_deps(self, ids: list[str]) -> dict[str, list[str]]:
        self.cursor.execute("EXECUTE select_deps (%s::uuid[])", (ids,))
        # NOTE: this might be intense for larger package managers
        flat = self.cursor.fetchall()

        # now, return this as a map for package id to list of dependencies
        result = defaultdict(list)
        for pkg_id, dep_id in flat:
            result[pkg_id].append(dep_id)
        return result


def larger_query(db: DB, root_package: str) -> Graph:
    graph = Graph()
    visited = set()
    leafs = set()

    # above sets will use the id of the package
    root_id = db.select_id(root_package)
    leafs.add(root_id)
    depth = 0

    while leafs - visited:
        query = list(leafs - visited)
        dependencies = db.select_deps(query)

        for pkg_id in query:
            i = graph.safely_add_node(pkg_id)
            js = graph.safely_add_nodes(dependencies[pkg_id])
            edges = [(i, j, None) for j in js]
            graph.add_edges_from(edges)
            leafs.update(dependencies[pkg_id])

        visited.update(query)
        depth += 1

    return graph


def display(graph: rx.PyDiGraph):
    try:
        sorted_nodes = rx.topological_sort(graph)
    except rx.DAGHasCycle as e:
        print(e)
        sorted_nodes = graph.node_indexes()

    headers = ["Package", "Dependencies", "Dependents"]
    data = []

    for node in sorted_nodes:
        data.append(
            [graph.nodes()[node], graph.out_degree(node), graph.in_degree(node)]
        )

    print(tabulate(data, headers=headers))


def draw(graph: rx.PyDiGraph, package: str):
    def color_edge(edge):
        out_dict = {
            # "color": f'"{edge_colors[edge]}"', <<<--- could attach to edge data!
            "color": "lightgrey",
            "fillcolor": "lightgrey",
            "penwidth": "0.05",
            "arrowsize": "0.05",
            "arrowhead": "tee",
            # "style": "invisible",
        }
        return out_dict

    def color_node(node):
        out_dict = {
            "label": "",
            "color": "lightblue",
            "shape": "circle",
            "style": "filled",
            "fillcolor": "lightblue",
            "width": "0.05",
            "height": "0.05",
            "fixedsize": "True",
        }
        return out_dict

    graph_attr = {
        "beautify": "True",
        "splines": "none",
    }

    graphviz_draw(
        graph,
        node_attr_fn=color_node,
        edge_attr_fn=color_edge,
        graph_attr=graph_attr,
        method="sfdp",
        filename=f"{package}.svg",
        image_type="svg",
    )


def latest(db: DB, package: str):
    G = larger_query(db, package)
    print("Generated graph")
    # display(G)
    draw(G, package)
    print("✅ Saved image")


if __name__ == "__main__":
    db = DB()

    parser = argparse.ArgumentParser()
    parser.add_argument("--package", help="The package to visualize")
    parser.add_argument(
        "--profile",
        help="Whether to profile the code",
        action="store_true",
        default=False,
    )
    args = parser.parse_args()
    package = args.package
    profile = args.profile

    if profile:
        profiler = cProfile.Profile()
        profiler.enable()

    latest(db, package)

    if profile:
        profiler.disable()
        stats = pstats.Stats(profiler).sort_stats(SortKey.TIME)
        stats.print_stats()
