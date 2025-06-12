#!/usr/bin/env pkgx +python@3.11 uv run

from collections import defaultdict, deque
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any
from uuid import UUID

import rustworkx as rx

from core.logger import Logger
from ranker.config import Config

logger = Logger("ranker.chai_graph")


@dataclass
class PackageNode:
    """Note that this is different from PackageInfo in main.py!
    This is based on canons!"""

    canon_id: UUID
    package_manager_ids: list[UUID] = field(default_factory=list)
    weight: Decimal = field(default_factory=Decimal)
    index: int = field(default_factory=lambda: -1)


class CHAI(rx.PyDiGraph):
    def __init__(self):
        super().__init__()
        self.canon_to_index: dict[UUID, int] = {}
        self.edge_to_index: dict[tuple[int, int], int] = {}

    def add_node(self, node: PackageNode) -> int:
        """Safely add a node to the graph. If exists, return the index"""
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

        iterations: int = 0

        while q:
            iterations += 1
            node_id, weight = q.popleft()

            # Ensure iteration count check happens regardless of other logic
            if iterations > max_iter:
                logger.warn(f"Max iterations reached: {max_iter}")
                break

            dependencies = self.successors(node_id)
            num_dependencies = len(dependencies)

            # If the weight arriving is already below tolerance, or if it's a terminal
            # node, add the entire weight to the result and stop distributing from
            # this node in this path.
            if num_dependencies == 0 or weight < tol:
                result[node_id] += weight
                continue

            # Handle non-terminal nodes with significant weight (weight >= tol)
            # Calculate the portion of weight the current node keeps.
            keep = weight * split_ratio

            # Always add the 'keep' amount to the node's result.
            # The tolerance check below is only for preventing further distribution
            # of insignificant amounts, not for deciding if the current node's
            # share is worth keeping.
            result[node_id] += keep

            # Calculate the total amount to be split among dependencies.
            split = weight - keep  # Equivalent to weight * (1 - split_ratio)

            # Calculate split per dependency.
            split_per_dep = split / num_dependencies

            # Use tolerance to gate further distribution: Only queue dependencies
            # if the amount they would receive individually is significant enough.
            if split_per_dep >= tol:
                for dep in dependencies:
                    q.append((dep.index, split_per_dep))
            # If split_per_dep < tol, the remaining 'split' amount is effectively
            # dropped from this distribution path, as it's deemed too small
            # to continue propagating. This helps prune the calculation.

        logger.log(f"Iterations: {iterations}. Ranks sum to {sum(result.values()):.9f}")

        return dict(result)
