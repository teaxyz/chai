#! /usr/bin/env pkgx +python@3.11 uv run

import json
import os
import pathlib
from dataclasses import dataclass
from typing import Dict, List, Tuple
from uuid import UUID

from core.logger import Logger
from graph.config import Config, load_config
from graph.db import GraphDB
from graph.rx_graph import CHAI, PackageNode

logger = Logger("graph.main")
config = load_config()
db = GraphDB()


@dataclass
class PackageInfo:
    id: UUID
    package_manager_id: UUID


def load_graph(
    legacy_pm_id: UUID,  # TODO: would probably be a list later
    package_to_canon_mapping: Dict[UUID, UUID],
    packages: List[PackageInfo],
    stop: int = None,
) -> CHAI:
    chai = CHAI()
    missing: set[Tuple[UUID, UUID]] = set()
    for i, package in enumerate(packages):
        # add this package's canon to the graph
        try:
            canon_id = package_to_canon_mapping[package.id]
        except KeyError:
            missing.add((package.id, package.package_manager_id))
            continue

        node = PackageNode(canon_id=canon_id)
        node.index = chai.add_node(node)

        # now grab its dependencies
        # there are two cases: legacy CHAI or new CHAI
        # the db handles these two distinctions
        if package.package_manager_id == legacy_pm_id:
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
                missing.add((dep, package.package_manager_id))
                continue

            dep_node = PackageNode(canon_id=dep_canon_id)
            dep_node.index = chai.add_node(dep_node)
            chai.add_edge(node.index, dep_node.index, {})

        if stop is not None and i >= stop:
            break

        if i % 1000 == 0:
            logger.log(
                f"{i}: Graph has {len(chai)} nodes and {len(chai.edge_to_index)} edges"
            )

    logger.log(f"Missing {len(missing)} packages")
    with open("missing.json", "w") as f:
        json.dump(list(missing), f)

    return chai


def save_ranks(ranks: Dict[UUID, float]) -> None:
    ranks_dir = pathlib.Path("graph/ranks")
    ranks_dir.mkdir(parents=True, exist_ok=True)

    # Find the highest existing rank index
    existing_files = [f for f in ranks_dir.glob("ranks_*.json")]
    next_index = 1
    if existing_files:
        indices = []
        for file in existing_files:
            try:
                index = int(file.stem.split("_")[1])
                indices.append(index)
            except (IndexError, ValueError):
                continue
        if indices:
            next_index = max(indices) + 1

    # Save the ranks file with the new index
    ranks_file = ranks_dir / f"ranks_{next_index}.json"
    with open(ranks_file, "w") as f:
        json.dump(ranks, f)
    logger.log(f"Saved ranks to {ranks_file}")

    # Create/update symlink to the latest ranks file
    latest_link = ranks_dir / "latest.json"
    if latest_link.exists():
        latest_link.unlink()
    os.symlink(ranks_file.name, latest_link)
    logger.log(f"Updated latest symlink to point to {ranks_file.name}")


def main(config: Config) -> None:
    package_to_canon: Dict[UUID, UUID] = db.get_package_to_canon_mapping()
    logger.log(f"{len(package_to_canon)} package to canon mappings")

    packages = [
        PackageInfo(id=id, package_manager_id=pm_id) for id, pm_id in db.get_packages()
    ]
    logger.log(f"{len(packages)} packages")

    chai = load_graph(config.pm_config.npm_pm_id, package_to_canon, packages, 1000)
    logger.log(f"CHAI has {len(chai)} nodes and {len(chai.edge_to_index)} edges")

    ranks = chai.pagerank(
        config.tearank_config.alpha, config.tearank_config.personalization
    )
    ranks = {chai[id].canon_id: rank for id, rank in ranks.items()}
    logger.log(f"Ranks have {len(ranks)} entries")

    save_ranks(ranks)


if __name__ == "__main__":
    main(config)
