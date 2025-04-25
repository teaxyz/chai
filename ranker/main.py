#! /usr/bin/env pkgx +python@3.11 uv run

# /// script
# dependencies = [
#   "numpy==2.2.3",
#   "rustworkx==0.16.0",
# ]
# ///

from dataclasses import dataclass
from typing import Dict, List, Tuple
from uuid import UUID

from core.logger import Logger
from core.models import TeaRank, TeaRankRun
from ranker.config import Config, load_config
from ranker.db import GraphDB
from ranker.dedupe import dedupe
from ranker.rx_graph import CHAI, PackageNode

logger = Logger("ranker.main")
config = load_config()
db = GraphDB(config.pm_config.npm_pm_id, config.pm_config.system_pm_ids)


@dataclass
class PackageInfo:
    id: UUID
    package_manager_id: UUID


def load_graph(
    config: Config,
    package_to_canon_mapping: Dict[UUID, UUID],
    packages: List[PackageInfo],
    stop: int = None,
) -> CHAI:
    chai = CHAI()
    missing: set[Tuple[UUID, UUID]] = set()
    npm_pm_id = config.pm_config.npm_pm_id

    for i, package in enumerate(packages):
        # add this package's canon to the graph
        try:
            canon_id = package_to_canon_mapping[package.id]
        except KeyError:
            missing.add((str(package.id), str(package.package_manager_id)))
            continue

        # grab the object from the graph if it exists
        if canon_id in chai.canon_to_index:
            node = chai[chai.canon_to_index[canon_id]]
        else:  # otherwise, create a new one
            node = PackageNode(canon_id=canon_id)
            node.index = chai.add_node(node)

        # add the package manager id to the node
        node.package_manager_ids.append(package.package_manager_id)

        # now grab its dependencies
        # there are two cases: legacy CHAI or new CHAI
        # the db helps us these two distinctions with two different helpers
        # TODO: eventually, CHAI will be at package to package, so everything will
        # "get_legacy_dependencies"
        if package.package_manager_id == npm_pm_id:
            dependencies = db.get_legacy_dependencies(package.id)
        else:
            dependencies = db.get_dependencies(package.id)

        # for each dependency, add the corresponding canon to the graph
        # and set the edge
        for dependency in dependencies:
            dep = dependency[0]
            try:
                dep_canon_id = package_to_canon_mapping[dep]
            except KeyError:
                missing.add((str(dep), str(package.package_manager_id)))
                continue

            dep_node = PackageNode(canon_id=dep_canon_id)
            dep_node.index = chai.add_node(dep_node)
            chai.add_edge(node.index, dep_node.index, {})

        if stop is not None and i >= stop:
            break

        if i % 1000 == 0:
            logger.debug(f"Processing package {i+1}/{len(packages)} (ID: {package.id})")

    logger.log(f"Missing {len(missing)} packages")
    # TODO: should we save the missing packages?

    return chai


def main(config: Config) -> None:
    # Call dedupe first
    dedupe(db)
    logger.log("✅ Deduplication finished, proceeding with TeaRank calculation.")

    # get the map of package_id -> canon_id
    package_to_canon: Dict[UUID, UUID] = db.get_package_to_canon_mapping()
    logger.log(f"{len(package_to_canon)} package to canon mappings")

    # get the list of packages
    packages = [
        PackageInfo(id=id, package_manager_id=pm_id) for id, pm_id in db.get_packages()
    ]
    logger.log(f"{len(packages)} packages")

    # load the graph
    chai = load_graph(config, package_to_canon, packages)
    logger.log(f"CHAI has {len(chai)} nodes and {len(chai.edge_to_index)} edges")

    # now, I need to generate the personalization vector
    canons_with_source_types: List[Tuple[UUID, List[UUID]]] = []
    for idx in chai.node_indexes():
        node = chai[idx]
        canons_with_source_types.append((node.canon_id, node.package_manager_ids))
    config.tearank_config.personalize(canons_with_source_types)

    # generate tea_ranks
    ranks = chai.distribute(
        config.tearank_config.personalization,
        config.tearank_config.split_ratio,
        config.tearank_config.tol,
        config.tearank_config.max_iter,
    )
    str_ranks = {str(chai[id].canon_id): f"{rank}" for id, rank in ranks.items()}

    # Determine the next run ID
    latest_run = db.get_current_tea_rank_run()
    current_run = latest_run.run + 1 if latest_run else 1
    logger.log(f"Starting TeaRank run number: {current_run}")

    # Prepare TeaRank objects with the *next* run ID
    tea_ranks = [
        TeaRank(canon_id=UUID(canon_id), tea_rank_run=current_run, rank=rank)
        for canon_id, rank in str_ranks.items()
    ]
    # Load all ranks first
    db.load_tea_ranks(tea_ranks)

    # Only after successfully loading ranks, load the corresponding run entry
    tea_rank_run = TeaRankRun(
        run=current_run, split_ratio=config.tearank_config.split_ratio
    )
    db.load_tea_rank_runs([tea_rank_run])
    logger.log("Done!")


if __name__ == "__main__":
    try:
        main(config)
    except Exception as e:
        logger.error(f"Some error occurred: {e}")
        raise
