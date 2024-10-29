import argparse
import cProfile
import pstats
from collections import defaultdict, deque
from os import getenv
from pstats import SortKey

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
            SELECT DISTINCT d.dependency_id FROM packages p \
            JOIN versions v ON p.id = v.package_id \
            JOIN dependencies d ON v.id = d.version_id \
            WHERE p.id = $1"
        )
        self.cursor.execute(
            "PREPARE select_deps_v2 AS \
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

    def select_name(self, id: int) -> str:
        self.cursor.execute("EXECUTE select_name (%s)", (id,))
        return self.cursor.fetchone()[0]

    def select_deps(self, id: int):
        self.cursor.execute("EXECUTE select_deps (%s)", (id,))
        for row in self.cursor.fetchall():
            yield row[0]

    def select_deps_v2(self, ids: list[int]) -> dict[str, list[str]]:
        self.cursor.execute("EXECUTE select_deps_v2 (%s::uuid[])", (ids,))
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

    while leafs - visited:
        query = list(leafs - visited)
        dependencies = db.select_deps_v2(query)

        for pkg_id in query:
            i = graph.safely_add_node(pkg_id)
            js = graph.safely_add_nodes(dependencies[pkg_id])
            edges = [(i, j, None) for j in js]
            graph.add_edges_from(edges)
            leafs.update(dependencies[pkg_id])

        visited.update(query)

    return graph


def cache_deps(db: DB, root_package: str) -> rx.PyDiGraph:
    """Simple BFS algorithm implementation"""
    graph = rx.PyDiGraph()
    queue = deque()
    visited = set()
    node_index_map: dict[str, int] = {}

    queue.append(root_package)

    while queue:
        pkg_name = queue.popleft()

        if pkg_name in visited:
            continue
        visited.add(pkg_name)

        # Get dependencies from the database
        pkg_id = db.select_id(pkg_name)  # always going to be new, i.e. not visited
        dependencies = db.select_deps(pkg_id)

        # Add the package to the graph
        if pkg_id not in node_index_map:
            pkg_index = graph.add_node(pkg_name)
            node_index_map[pkg_id] = pkg_index
        else:
            pkg_index = node_index_map[pkg_id]

        for dep_id in dependencies:
            # if I know the dependency, I can skip the query
            if dep_id not in node_index_map:
                dep_name = db.select_name(dep_id)
                dep_index = graph.add_node(dep_name)
                node_index_map[dep_id] = dep_index
            else:
                dep_index = node_index_map[dep_id]
                dep_name = graph[dep_index]

            # Add an edge from the package to its dependency
            graph.add_edge(pkg_index, dep_index, None)

            # Enqueue the dependency for processing
            if dep_name not in visited:
                queue.append(dep_name)

    return graph


def basic_bfs(db: DB, root_package: str) -> rx.PyDiGraph:
    """Simple BFS algorithm implementation"""
    graph = rx.PyDiGraph()
    queue = deque()
    visited = set()

    queue.append(root_package)

    while queue:
        package = queue.popleft()

        if package in visited:
            continue
        visited.add(package)

        # Add the package to the graph
        # TODO: not sure if this is optimal?
        if package not in graph.nodes():
            package_index = graph.add_node(package)
        else:
            package_index = graph.nodes().index(package)

        # Get dependencies from the database
        package_id = db.select_id(package)  # always going to be new, i.e. not visited
        dependencies = db.select_deps(package_id)

        for dep_id in dependencies:
            # TODO: if I know the dependency, I can skip the query
            dep_name = db.select_name(dep_id)

            # Add the dependency node if not already in the graph
            if dep_name not in graph.nodes():
                dep_index = graph.add_node(dep_name)
            else:
                dep_index = graph.nodes().index(dep_name)

            # Add an edge from the package to its dependency
            graph.add_edge(package_index, dep_index, None)

            # Enqueue the dependency for processing
            if dep_name not in visited:
                queue.append(dep_name)

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


def draw(graph: rx.PyDiGraph):
    def color_edge(edge):
        out_dict = {
            # "color": f'"{edge_colors[edge]}"', <<<--- could attach to edge data!
            "color": "lightgrey",
            "fillcolor": "lightgrey",
            "penwidth": str(0.01),
            "arrowsize": str(0.05),
            "arrowhead": "tee",
        }
        return out_dict

    def color_node(node):
        out_dict = {
            # "label": str(node),
            "label": "",
            "color": "lightblue",
            # "pos": f'"{pos[node][0]}, {pos[node][1]}"',
            # "fontname": '"DejaVu Sans"',
            # "pin": "True",
            "shape": "circle",
            "style": "filled",
            "fillcolor": "lightblue",
            "width": "0.1",
            "height": "0.1",
            "fixedsize": "True",
        }
        return out_dict

    graph_attr = {
        "beautify": "True",
        "resolution": "300",
        "ratio": "fill",
    }

    graphviz_draw(
        graph,
        node_attr_fn=color_node,
        edge_attr_fn=color_edge,
        graph_attr=graph_attr,
        method="sfdp",
        filename=f"{package}.svg",
    )


def latest(db: DB, package: str):
    G = larger_query(db, package)
    print("Generated graph")
    # display(G)
    draw(G)
    print("✅ Saved image")


def version_2(db: DB, package: str):
    G = cache_deps(db, package)
    # display(G)


def version_1(db: DB, package: str):
    G = basic_bfs(db, package)
    # display(G)


if __name__ == "__main__":
    db = DB()

    parser = argparse.ArgumentParser()
    parser.add_argument("package", help="The package to visualize")
    args = parser.parse_args()
    package = args.package

    # Add profiling
    profiler = cProfile.Profile()
    profiler.enable()

    latest(db, package)

    profiler.disable()
    stats = pstats.Stats(profiler).sort_stats(SortKey.TIME)
    stats.print_stats()
