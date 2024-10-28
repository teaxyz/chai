import argparse
from collections import deque
from os import getenv

import PIL
import psycopg2
import rustworkx as rx
from rustworkx.visualization import graphviz_draw
from tabulate import tabulate

CHAI_DATABASE_URL = getenv("CHAI_DATABASE_URL")


class Package:
    id: str
    name: str

    def __hash__(self) -> int:
        return hash(self.id)

    def __eq__(self, other: "Package") -> bool:
        return self.id == other.id


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


def build_dependency_graph(db: DB, root_package: str):
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


def draw(graph: rx.PyDiGraph, labels: dict[int, str]) -> PIL.Image:
    # mpl_draw(graph, labels=str, with_labels=True)
    return graphviz_draw(graph, graph_attr={"label": "Dependency Graph"})


def main(db: DB, package: str):
    G = build_dependency_graph(db, package)
    node_map = {i: G.nodes()[i] for i in G.node_indexes()}

    # display(G)
    image = draw(G, node_map)
    image.show()
    # image.save("graph.png")


if __name__ == "__main__":
    db = DB()

    parser = argparse.ArgumentParser()
    parser.add_argument("package", help="The package to visualize")
    args = parser.parse_args()

    package = args.package
    main(db, package)
