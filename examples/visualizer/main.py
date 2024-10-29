import argparse
from collections import deque
from os import getenv
from typing import Generator

import PIL
import psycopg2
import rustworkx as rx
from rustworkx.visualization import graphviz_draw
from tabulate import tabulate

CHAI_DATABASE_URL = getenv("CHAI_DATABASE_URL")

graph_attr = {
    "arrowsize": 0.5,
    "beautify": True,
    "labelfontsize": 5,
    "size": "10,10",
}


class Package:
    id: str
    name: str
    index: int

    def __init__(self, id: str, name: str):
        self.index = None
        self.id = id
        self.name = name

    def __str__(self) -> str:
        return f"Package({self.name} @ {self.index})"


def node_label(node: Package, empty: bool = True) -> str:
    return {"label": node.name} if not empty else {"label": ""}


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
            SELECT d.dependency_id FROM packages p \
            JOIN versions v ON p.id = v.package_id \
            JOIN dependencies d ON v.id = d.version_id \
            WHERE p.name = $1"
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

    def select_deps_v2(self, name: str) -> Generator[str, None, None]:
        self.cursor.execute("EXECUTE select_deps_v2 (%s)", (name,))
        for row in self.cursor.fetchall():
            yield row[0]


def build_dependency_graph(db: DB, root_package: str):
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


def draw(graph: rx.PyDiGraph) -> PIL.Image:
    return graphviz_draw(graph, node_attr_fn=node_label, method="sfdp")


def main(db: DB, package: str):
    G = build_dependency_graph(db, package)

    display(G)
    # image = draw(G, node_map)
    # # image.show()
    # image.save("graph.png")


if __name__ == "__main__":
    db = DB()

    parser = argparse.ArgumentParser()
    parser.add_argument("package", help="The package to visualize")
    args = parser.parse_args()

    package = args.package
    main(db, package)
