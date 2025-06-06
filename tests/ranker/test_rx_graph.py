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
TOLERANCE = Decimal("1e-9")
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
    assert abs(sum(personalization.values()) - Decimal(1.0)) <= TOLERANCE, \
        "Initial personalization should sum to 1 within tolerance"

    # Add random edges (potential cycles)
    node_indices = list(G.node_indices())
    for u_idx in node_indices:
        for v_idx in node_indices:
            if u_idx != v_idx and random.random() < EDGE_PROBABILITY:
                G.add_edge(u_idx, v_idx, None)  # Edge data is not used in distribute

    return G, personalization


@pytest.mark.ranker
@pytest.mark.slow
class TestDistributeConservation:
    """Test the CHAI graph distribute function for weight conservation."""
    
    def test_distribute_conservation(
        self,
        large_chai_graph: tuple[CHAI, dict[uuid.UUID, Decimal]],
    ):
        """
        Tests that the distribute function conserves weight approximately.

        The final sum of ranks should be less than or equal to the initial sum.
        Due to the tolerance threshold stopping distribution paths, some weight might
        be "lost" (not distributed further), so the final sum might be slightly
        less than the initial sum (1.0). The difference should ideally be small,
        related to the tolerance value.
        """
        G, personalization = large_chai_graph

        initial_sum = sum(personalization.values())
        assert initial_sum == pytest.approx(Decimal(1.0))

        ranks = G.distribute(
            personalization=personalization,
            split_ratio=SPLIT_RATIO,
            tol=TOLERANCE,
            max_iter=MAX_ITER,
        )

        final_sum = sum(ranks.values())

        # Basic assertions
        assert final_sum > Decimal(0.0), "Final sum of ranks must be positive"
        assert (
            final_sum <= initial_sum
        ), "Final sum should not exceed initial sum (direct comparison)"

        # Check if the difference is within a reasonable bound
        lost_weight = initial_sum - final_sum
        print(f"Initial Sum: {initial_sum:.15f}")
        print(f"Final Sum:   {final_sum:.15f}")
        print(f"Lost Weight: {lost_weight:.15f}")
        print(f"Tolerance:   {TOLERANCE:.15f}")

        # Assert the lost weight isn't excessively large
        # A simple check could be that it's less than a small fraction of the initial sum
        assert lost_weight < initial_sum * Decimal(
            "0.1"
        ), "Lost weight should be a small fraction of the initial sum"
