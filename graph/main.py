#! /usr/bin/env pkgx +python@3.11 uv run

import json
import os
import pathlib
from dataclasses import dataclass
from decimal import Decimal, getcontext
from typing import Dict, List, Tuple
from uuid import UUID

import numpy as np

from core.logger import Logger
from graph.config import Config, load_config
from graph.db import GraphDB
from graph.rx_graph import CHAI, PackageNode

logger = Logger("graph.main")
config = load_config()
db = GraphDB(config.pm_config.npm_pm_id, config.pm_config.system_pm_ids)

# data directory for ranks
RANKS_DIR = pathlib.Path("data/ranker/ranks")
RANKS_DIR.mkdir(parents=True, exist_ok=True)


def generate_run_id() -> str:
    """generates the run_id based on the number of files in the ranks directory"""
    existing_files = [f for f in RANKS_DIR.glob("ranks_*.json")]
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
    return f"{next_index}"


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
        # the db handles these two distinctions
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
            logger.log(
                f"{i}: Graph has {len(chai)} nodes and {len(chai.edge_to_index)} edges"
            )

    logger.log(f"Missing {len(missing)} packages")
    with open(RANKS_DIR / "missing.json", "w") as f:
        json.dump(list(missing), f)

    return chai


def save_ranks(ranks: Dict[str, float], run_id: str) -> None:
    # Save the ranks file with the new index
    ranks_file = RANKS_DIR / f"ranks_{run_id}.json"
    with open(ranks_file, "w") as f:
        json.dump(ranks, f)
    logger.log(f"Saved ranks to {ranks_file}")

    # Create/update symlink to the latest ranks file
    latest_link = RANKS_DIR / "latest.json"
    if latest_link.exists():
        latest_link.unlink()
    os.symlink(ranks_file.name, latest_link)
    logger.log(f"Updated latest symlink to point to {ranks_file.name}")


def main(config: Config, run_id: str) -> None:
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
    getcontext().prec = 9
    split_ratios = [0.2, 0.25, 0.3, 0.35, 0.4, 0.45, 0.5, 0.55]
    for split_ratio in split_ratios:
        decimal_split_ratio = Decimal(split_ratio)
        ranks = chai.distribute(
            config.tearank_config.personalization,
            decimal_split_ratio,
            config.tearank_config.tol,
            config.tearank_config.max_iter,
        )
        str_ranks = {str(chai[id].canon_id): f"{rank}" for id, rank in ranks.items()}

        # save the ranks
        save_ranks(str_ranks, f"{run_id}_{split_ratio}")


if __name__ == "__main__":
    i = generate_run_id()
    main(config, i)
