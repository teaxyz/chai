#! /usr/bin/env pkgx +python@3.11 uv run

import json
from decimal import Decimal
from uuid import UUID

from core.logger import Logger
from core.models import Canon
from graph.chai_graph import CHAI, Package
from graph.db import GraphDB
from graph.params import PageRankParams

logger = Logger("graph.main")


def load_graph(elements: list[Canon], package_to_canon: dict[str, str]) -> CHAI:
    C = CHAI()
    missing = set()
    for canon in elements:
        i = C.add_node(Package(canon_id=canon.id))
        C[i].index = i

        for dependency in db.get_package_dependencies(canon.id):
            dep = dependency[0]
            try:
                dep_canon_id = package_to_canon[dep]
            except KeyError:
                missing.add(str(dep))
                # TODO: we could probably keep track of it in the graph, and measure
                # its relative importance?
                continue
            j = C.add_node(Package(canon_id=dep_canon_id))
            C[j].index = j
            C.add_edge(i, j, None)

    logger.warn(f"Ignored {len(missing)} dependencies")
    with open("missing.json", "w") as f:
        json.dump(list(missing), f)

    logger.log(f"Graph has {len(C)} nodes, {len(C.edges())} edges")

    return C


def rank(graph: CHAI, params: PageRankParams) -> dict[UUID, Decimal]:
    # graph.update_weights(params.initial_weights)
    ranks = graph.pagerank(params.personalization)
    result = {str(graph[id].canon_id): rank for id, rank in ranks.items()}
    sorted_result = sorted(result.items(), key=lambda x: x[1], reverse=True)
    return sorted_result


def main(db: GraphDB, params: PageRankParams):
    # get the source data
    canons = db.get_canons()
    logger.log(f"Found {len(canons)} canons")
    
    package_to_canon = db.get_canon_packages()
    logger.log(f"Found {len(package_to_canon)} package to canon mappings")

    C = load_graph(canons, package_to_canon)
    C.npm(db)
    logger.log(f"Graph has {len(C)} nodes, {len(C.edges())} edges")

    result = rank(C, params)
    logger.log(f"Ranked {len(result)} packages")

    # write the ranks
    with open("ranks_typescript.json", "w") as f:
        json.dump(result, f)


if __name__ == "__main__":
    # with cProfile.Profile() as pr:
    db = GraphDB()
    params = PageRankParams(db)
    params.personalize()
    main(db, params)
    # pr.print_stats("cumtime")
