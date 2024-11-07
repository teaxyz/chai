import argparse
import cProfile
import pstats
from os import getenv
from pstats import SortKey

import psycopg2
import rustworkx as rx
from rustworkx.visualization import graphviz_draw
from tabulate import tabulate

CHAI_DATABASE_URL = getenv("CHAI_DATABASE_URL")


class Package:
    id: str
    name: str
    pagerank: float

    def __init__(self, id: str):
        self.id = id
        self.name = ""
        self.pagerank = 0
        self.depth = 9999

    def __str__(self):
        return self.name


class Graph(rx.PyDiGraph):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.node_index_map: dict[Package, int] = {}
        self._package_cache: dict[str, Package] = {}

    def _get_or_create_package(self, pkg_id: str) -> Package:
        if pkg_id not in self._package_cache:
            pkg = Package(pkg_id)
            self._package_cache[pkg_id] = pkg
        return self._package_cache[pkg_id]

    def safely_add_node(self, pkg_id: str) -> int:
        pkg = self._get_or_create_package(pkg_id)
        if pkg not in self.node_index_map:
            index = super().add_node(pkg)
            self.node_index_map[pkg] = index
            return index
        return self.node_index_map[pkg]

    def safely_add_nodes(self, nodes: list[str]) -> list[int]:
        return [self.safely_add_node(node) for node in nodes]

    def pagerank(self) -> None:
        pageranks = rx.pagerank(self)
        for index in self.node_indexes():
            self[index].pagerank = pageranks[index]


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
            SELECT DISTINCT p.id, p.name, d.dependency_id FROM packages p \
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

    def select_deps(self, ids: list[str]) -> dict[str, dict[str, str | set[str]]]:
        self.cursor.execute("EXECUTE select_deps (%s::uuid[])", (ids,))
        # NOTE: this might be intense for larger package managers
        flat = self.cursor.fetchall()
        # now, return this as a map capturing the package name and its dependencies
        result = {}
        for pkg_id, pkg_name, dep_id in flat:
            # add the package if it doesn't already exist in result
            if pkg_id not in result:
                result[pkg_id] = {"name": pkg_name, "dependencies": set()}
            # add the dependency to the dependencies set
            result[pkg_id]["dependencies"].add(dep_id)

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
        no_deps = set()

        for pkg_id in query:
            i = graph.safely_add_node(pkg_id)
            graph[i].depth = min(depth, graph[i].depth)
            if pkg_id in dependencies:
                graph[i].name = dependencies[pkg_id]["name"]
                js = graph.safely_add_nodes(dependencies[pkg_id]["dependencies"])
                edges = [(i, j, None) for j in js]
                graph.add_edges_from(edges)
                leafs.update(dependencies[pkg_id]["dependencies"])
            else:
                no_deps.add(pkg_id)

        visited.update(query)
        depth += 1

    print(f"{len(no_deps)} don't have dependencies")  # TODO: add their names
    return graph


def display(graph: rx.PyDiGraph):
    sorted_nodes = sorted(graph.node_indexes(), key=lambda x: graph[x].depth)
    headers = ["Package", "First Depth", "Dependencies", "Dependents", "Pagerank"]
    data = []

    for node in sorted_nodes:
        data.append(
            [
                graph[node],
                graph[node].depth,
                f"{graph.out_degree(node):,}",
                f"{graph.in_degree(node):,}",
                f"{graph[node].pagerank:.8f}",
            ]
        )

    print(tabulate(data, headers=headers))


def draw(graph: rx.PyDiGraph, package: str):
    total_nodes = graph.num_nodes()
    total_edges = graph.num_edges()
    depth_color_map = {
        0: "red",
        1: "lightblue",
        2: "lightgreen",
        3: "orange",
        4: "purple",
    }

    def color_edge(edge):
        out_dict = {
            "color": "lightgrey",
            "fillcolor": "lightgrey",
            "penwidth": "0.05",
            "arrowsize": "0.05",
            "arrowhead": "tee",
        }
        return out_dict

    def color_node(node: Package):
        scale = 20

        def label_nodes(node: Package):
            if node.pagerank > 0.01:
                return f"{node.name}"
            return ""

        def size_center_node(node: Package):
            if node.depth == 0:
                return "1"
            return str(node.pagerank * scale)

        out_dict = {
            "label": label_nodes(node),
            "fontsize": "5",
            "color": depth_color_map.get(node.depth, "lightgrey"),
            "shape": "circle",
            "style": "filled",
            "fixedsize": "True",
            "width": size_center_node(node),
            "height": size_center_node(node),
        }
        return out_dict

    label = f"<{package} <br/>nodes: {str(total_nodes)} <br/>edges: {str(total_edges)}>"
    graph_attr = {
        "beautify": "True",
        "splines": "none",
        "overlap": "0.01",
        "label": label,
        "labelloc": "t",
        "labeljust": "l",
        "fontname": "Menlo",
    }

    graphviz_draw(
        graph,
        node_attr_fn=color_node,
        edge_attr_fn=color_edge,
        graph_attr=graph_attr,
        method="twopi",  # NOTE: sfdp works as well
        filename=f"{package}.svg",
        image_type="svg",
    )


def latest(db: DB, package: str):
    G = larger_query(db, package)
    G.pagerank()
    display(G)
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
