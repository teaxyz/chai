#!/usr/bin/env pkgx +python@3.11 uv run

from collections import deque
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any
from uuid import UUID

import rustworkx as rx

from core.logger import Logger
from graph.db import GraphDB

logger = Logger("graph.chai_graph")


@dataclass
class PackageNode:
    """Note that this is different from PackageInfo in main.py!
    This is based on canons!"""

    canon_id: UUID
    weight: Decimal = field(default_factory=Decimal)
    index: int = field(default_factory=lambda: -1)


class CHAI(rx.PyDiGraph):
    def __init__(self):
        super().__init__()
        self.canon_to_index: dict[UUID, int] = {}
        self.edge_to_index: dict[tuple[int, int], int] = {}

    def add_node(self, node: PackageNode) -> int:
        """Safely add a note to the graph. If exists, return the index"""
        if node.canon_id not in self.canon_to_index:
            index = super().add_node(node)
            self.canon_to_index[node.canon_id] = index
        return self.canon_to_index[node.canon_id]

    def add_edge(self, u: int, v: int, edge_data: Any) -> None:
        """Safely add an edge to the graph. If exists, return the index"""
        if (u, v) not in self.edge_to_index:
            index = super().add_edge(u, v, edge_data)
            self.edge_to_index[(u, v)] = index
        return self.edge_to_index[(u, v)]

    def generate_personalization(
        self, personalization: dict[UUID, Decimal]
    ) -> dict[int, float]:
        result = {}
        for id, weight in personalization.items():
            if id not in self.canon_to_index:
                continue
            result[self.canon_to_index[id]] = float(weight)
        return result

    def pagerank(
        self, alpha: Decimal, personalization: dict[UUID, Decimal]
    ) -> dict[UUID, Decimal]:
        return rx.pagerank(
            self,
            alpha=float(alpha),
            personalization=self.generate_personalization(personalization),
        )

    def npm(self, db: GraphDB):
        typescript_canon_id = UUID("cfedea77-b927-4742-a4f3-55e7d07d3d67")
        typescript_package_id = UUID("58cac3e5-5e62-48a1-a88b-cbbf67fa6fa2")

        typescript_index = self.canon_to_index[typescript_canon_id]

        queue = deque()
        queue.append(typescript_package_id)
        visited = set()

        while queue:
            # logger.log(f"Queue: {len(queue)}. Visited: {len(visited)}")
            package_id = queue.popleft()

            # if we've already seen the node before, we can move on to the next one
            if package_id in visited:
                continue

            # step 1 is to add it to the graph
            if package_id != typescript_package_id:
                # this if is present because typescript has a canon ID
                # but we're working with package ids for the rest of it
                i = self.add_node(PackageNode(canon_id=package_id))

                # note that this happens EVERY time except the first instance
                # (when typescript is already loaded)
            else:
                i = typescript_index

            dependencies = db.get_npm_dependencies(package_id)

            for dependency in dependencies:
                dep_package = PackageNode(canon_id=dependency[0])

                # add it to the graph
                dep_package.index = self.add_node(dep_package)

                # add the edge
                self.add_edge(i, dep_package.index, None)

                # add it to the queue
                queue.append(dependency[0])

            visited.add(package_id)

    def distribute(self):
        # 80 / 20
        # distribute by rank for the bottom 20
        pass
