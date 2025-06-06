"""
Test the CHAI graph ranking algorithm.

This module tests the rx_graph module which implements a custom graph-based
ranking algorithm for packages. The tests focus on verifying that the
distribute function conserves weight appropriately.
"""

import random
import uuid
from decimal import Decimal

import pytest

from ranker.rx_graph import CHAI, PackageNode

# Constants for the test
NUM_NODES = 100000
EDGE_PROBABILITY = 0.001
SPLIT_RATIO = Decimal("0.85")
TOLERANCE = Decimal("1e-6")
MAX_ITER = 10000000


@pytest.fixture
def large_chai_graph() -> tuple[CHAI, dict[uuid.UUID, Decimal]]:
    """Creates a large CHAI graph with random edges and personalization."""
    G = CHAI()
    nodes = []
    initial_personalization_raw = {}

    # Create nodes
    for i in range(NUM_NODES):
        canon_id = uuid.uuid4()
        node = PackageNode(canon_id=canon_id)
        node.index = G.add_node(node)
        nodes.append(node)
        # Assign random initial weight for personalization
        initial_personalization_raw[canon_id] = Decimal(random.random())

    # Normalize personalization to sum to 1
    total_weight = sum(initial_personalization_raw.values())
    personalization = {
        uid: weight / total_weight
        for uid, weight in initial_personalization_raw.items()
    }
    assert (
        abs(sum(personalization.values()) - Decimal(1.0)) <= TOLERANCE
    ), f"Initial personalization should sum to 1 within tolerance: {sum(personalization.values())}"  # noqa: E501

    # Add random edges (potential cycles)
    node_indices = list(G.node_indices())
    for u_idx in node_indices:
        for v_idx in node_indices:
            if u_idx != v_idx and random.random() < EDGE_PROBABILITY:
                G.add_edge(u_idx, v_idx, None)  # Edge data is not used in distribute

    return G, personalization
