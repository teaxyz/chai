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
    depth: int | None

    def __init__(self, id: str):
        self.id = id
        self.name = ""
        self.pagerank = 0
        self.depth = None

    def __str__(self):
        return self.name


class Graph(rx.PyDiGraph):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.node_index_map: dict[Package, int] = {}
        self._package_cache: dict[str, Package] = {}

    # The data model has IDs, but rustworkx uses indexes
    # Good news - it can index by object. So, we're just keeping track of that
    def _get_or_create_package(self, pkg_id: str) -> Package:
        """A cache to avoid creating the same package multiple times"""
        if pkg_id not in self._package_cache:
            pkg = Package(pkg_id)
            self._package_cache[pkg_id] = pkg
        return self._package_cache[pkg_id]

    def safely_add_node(self, pkg_id: str) -> int:
        """Adds a node to the graph if it doesn't already exist"""
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

    def nameless_nodes(self) -> list[str]:
        return [self[i].id for i in self.node_indexes() if self[i].name == ""]

    def max_depth(self) -> int:
        return max([self[i].depth for i in self.node_indexes()])


class DB:
    """Prepares the sql statements and connects to the database"""

    def __init__(self):
        self.connect()
        self.cursor.execute(
            "PREPARE select_id AS SELECT id FROM packages WHERE name = $1"
        )
        self.cursor.execute(
            "PREPARE select_name AS SELECT id, name FROM packages WHERE id = ANY($1)"
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
        # NOTE: this might be intense for larger package managers
        # NOTE: I have to cast the list to a uuid[] for psycopg2 to correctly handle it
        self.cursor.execute("EXECUTE select_deps (%s::uuid[])", (ids,))
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

    def select_name(self, ids: list[str]) -> list[tuple[str, str]]:
        self.cursor.execute("EXECUTE select_name (%s::uuid[])", (ids,))
        return self.cursor.fetchall()


def larger_query(db: DB, root_package: str, max_depth: int) -> Graph:
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

        # Increment the depth, and get out if too much
        depth += 1
        if depth > max_depth:
            # Set the depth for the remaining leafs
            for pkg_id in query:
                i = graph.safely_add_node(pkg_id)
                graph[i].depth = depth
            break

        for pkg_id in query:
            i = graph.safely_add_node(pkg_id)

            # Have we encountered this node before? If not, set the depth
            if graph[i].depth is None:
                graph[i].depth = depth

            if pkg_id in dependencies:
                graph[i].name = dependencies[pkg_id]["name"]
                js = graph.safely_add_nodes(dependencies[pkg_id]["dependencies"])
                edges = [(i, j, None) for j in js]
                graph.add_edges_from(edges)
                leafs.update(dependencies[pkg_id]["dependencies"])

        visited.update(query)

    # Add the names for the packages that don't have dependencies
    nameless_nodes = graph.nameless_nodes()
    names = db.select_name(nameless_nodes)
    for pkg_id, pkg_name in names:
        i = graph.safely_add_node(pkg_id)
        graph[i].name = pkg_name

    return graph


def display(graph: Graph):
    sorted_nodes = sorted(graph.node_indexes(), key=lambda x: graph[x].depth)
    headers = ["Package", "First Depth", "Dependencies", "Dependents", "Pagerank"]
    data = []

    for node in sorted_nodes:
        data.append(
            [
                graph[node],
                graph[node].depth,
                graph.out_degree(node),
                graph.in_degree(node),
                graph[node].pagerank,
            ]
        )

    print(tabulate(data, headers=headers, floatfmt=".8f", intfmt=","))


def draw(graph: Graph, package: str, img_type: str = "svg"):
    ALLOWABLE_FILE_TYPES = ["svg", "png"]
    if img_type not in ALLOWABLE_FILE_TYPES:
        raise ValueError(f"file type must be one of {ALLOWABLE_FILE_TYPES}")

    max_depth = graph.max_depth()
    total_nodes = graph.num_nodes()
    total_edges = graph.num_edges()

    def depth_to_grayscale(depth: int) -> str:
        """Convert depth to a grayscale color."""
        if depth == 1:
            return "red"
        return f"grey{depth + 10 + (depth - 1) // 9}"

    # Unused because I don't visualize edges
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
            if node.depth == 1:
                return "1"
            return str(node.pagerank * scale)

        out_dict = {
            "label": label_nodes(node),
            "fontsize": "5",
            "fontcolor": "grey",
            "fontname": "Menlo",
            "color": depth_to_grayscale(node.depth),
            "shape": "circle",
            "style": "filled",
            "fixedsize": "True",
            "width": size_center_node(node),
            "height": size_center_node(node),
        }
        return out_dict

    label = f"<{package} (big red dot) <br/>depth: {max_depth} <br/>nodes: {str(total_nodes)} <br/>edges: {str(total_edges)}>"  # noqa: E501
    graph_attr = {
        "beautify": "True",
        "splines": "none",
        "overlap": "0",
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
        filename=f"{package}.{img_type}",
        image_type=img_type,
    )


def latest(db: DB, package: str, depth: int, img_type: str):
    G = larger_query(db, package, depth)
    G.pagerank()
    display(G)
    draw(G, package, img_type)
    print("✅ Saved image")


if __name__ == "__main__":
    db = DB()

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--package", help="The package to visualize", type=str, required=True
    )
    parser.add_argument(
        "--depth", help="Maximum depth to go to", type=int, default=9999
    )
    parser.add_argument(
        "--profile", help="Performance!", action="store_true", default=False
    )
    parser.add_argument(
        "--image-type",
        help="The file type to save the image as",
        type=str,
        default="svg",
    )
    args = parser.parse_args()
    package = args.package
    depth = args.depth
    profile = args.profile
    img_type = args.image_type

    if profile:
        profiler = cProfile.Profile()
        profiler.enable()

    latest(db, package, depth, img_type)

    if profile:
        profiler.disable()
        stats = pstats.Stats(profiler).sort_stats(SortKey.TIME)
        stats.print_stats()
