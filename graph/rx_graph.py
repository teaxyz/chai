#!/usr/bin/env pkgx +python@3.11 uv run

from collections import defaultdict, deque
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, List
from uuid import UUID

import rustworkx as rx

from core.logger import Logger
from graph.config import Config
from graph.db import GraphDB

logger = Logger("graph.chai_graph")

qsv_canon = UUID("0753ab2c-c003-4a27-ae72-e6a337aaebb2")
qsv_package = UUID("1749e330-96a4-405a-9e90-3770878f4fa8")


@dataclass
class PackageNode:
    """Note that this is different from PackageInfo in main.py!
    This is based on canons!"""

    canon_id: UUID
    package_manager_ids: List[UUID] = field(default_factory=list)
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
    ) -> rx.CentralityMapping:
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

    def pretty_print(self, results: dict[int, Decimal]) -> None:
        if not logger.is_verbose():
            return

        logger.debug("***** RESULTS ******")
        for i, rank in results.items():
            logger.debug(f"\tNode {i} has rank {rank:.9f}")
        logger.debug(f"Sum of results: {sum(results.values()):.9f}")

    def pretty_print_queue(self, q: deque[tuple[int, Decimal]]) -> None:
        if not logger.is_verbose():
            return

        logger.debug("***** QUEUE ******")
        for node_id, weight in q:
            logger.debug(f"\tNode {node_id} has weight {weight:.9f}")
        logger.debug(f"Sum of queue: {sum(weight for _, weight in q):.9f}")

    def distribute(
        self,
        personalization: dict[UUID, Decimal],
        split_ratio: Decimal,
        tol: Decimal,
        max_iter: int = 100,
    ) -> dict[int, Decimal]:
        """Distribute values across the graph based on dependencies."""
        if not personalization:
            raise ValueError("Personalization is empty")

        # Convert personalization to index-based dict
        result = defaultdict(Decimal)
        q: deque[tuple[int, Decimal]] = deque()

        for id, weight in personalization.items():
            if id not in self.canon_to_index:
                logger.log(f"{id} is type {type(id)}")
                raise ValueError(f"Canon ID {id} not found in CHAI")
            q.append((self.canon_to_index[id], weight))

        logger.log("***** INITIAL QUEUE ******")
        logger.log(f"Sum of personalization: {sum(personalization.values()):.9f}")
        iterations: int = 0

        while q:
            iterations += 1
            node_id, weight = q.popleft()

            # first, calculate keep
            # if you have no dependencies, you don't split
            dependencies = self.successors(node_id)
            num_dependencies = len(dependencies)
            if num_dependencies == 0:
                keep = weight
            else:  # otherwise, you split
                keep = weight * split_ratio

            # second, check if we should continue for keep
            if keep < tol:
                continue

            # we can continue -> add keep to result
            result[node_id] += keep

            # third, deal with splits
            # if you have no dependencies, there's nothing to split so move on
            if num_dependencies == 0:
                continue

            # otherwise, calculate split
            split = (weight - keep) / num_dependencies
            for dep in dependencies:
                q.append((dep.index, split))

            if iterations > max_iter:
                logger.warn(f"Max iterations reached: {max_iter}")
                break

        logger.log(f"Iterations: {iterations}. Ranks sum to {sum(result.values()):.9f}")

        return dict(result)


if __name__ == "__main__":
    config = Config()
    G = CHAI()
    # create a simple graph
    uuid_1 = UUID("123e4567-e89b-12d3-a456-426614174000")
    uuid_2 = UUID("123e4567-e89b-12d3-a456-426614174001")
    uuid_3 = UUID("123e4567-e89b-12d3-a456-426614174002")
    uuid_4 = UUID("123e4567-e89b-12d3-a456-426614174003")

    a = PackageNode(canon_id=uuid_1)
    b = PackageNode(canon_id=uuid_2)
    c = PackageNode(canon_id=uuid_3)
    d = PackageNode(canon_id=uuid_4)

    a.index = G.add_node(a)
    b.index = G.add_node(b)
    c.index = G.add_node(c)
    d.index = G.add_node(d)

    G.add_edge(a.index, b.index, None)
    G.add_edge(a.index, c.index, None)
    G.add_edge(b.index, d.index, None)

    initial_personalization = {
        uuid_1: Decimal(0.3),  # Homebrew
        uuid_2: Decimal(0.3),  # Homebrew
        uuid_3: Decimal(0.4),  # Homebrew
        uuid_4: Decimal(0.0),  # crates
    }

    ranks = G.distribute(
        initial_personalization,
        config.tearank_config.split_ratio,
        config.tearank_config.tol,
        max_iter=2,
    )
    for i, rank in ranks.items():
        print(f"\tNode {i} has rank {rank:.9f}")
    print(f"Sum of results: {sum(ranks.values()):.9f}")
