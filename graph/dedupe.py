#!/usr/bin/env uv run

from dataclasses import dataclass
from os import getenv
from uuid import UUID, uuid4

from core.logger import Logger
from core.models import Canon, CanonPackage
from graph.db import GraphDB

LOAD = getenv("LOAD", "false").lower() == "true"
logger = Logger("graph_main")


@dataclass
class DedupedPackage:
    package_id: UUID
    name: str
    url: str
    canonical_package_id: UUID | None


def convert_deduped_to_canonical(deduped: DedupedPackage) -> Canon:
    return Canon(id=deduped.canonical_package_id, name=deduped.name)


def generate_mapping(
    canon_id: UUID, packages: list[DedupedPackage]
) -> list[CanonPackage]:
    return [
        CanonPackage(id=uuid4(), canon_id=canon_id, package_id=pkg.package_id)
        for pkg in packages
    ]


def dedupe(db: GraphDB):
    data = db.get_packages_with_urls()
    logger.log(f"Collected {len(data)} packages")

    url_map: dict[str, list[DedupedPackage]] = {}

    # build the url map
    for row in data:
        url = row.url

        # if homepage is not set, skip
        if not url:
            continue

        pkg = DedupedPackage(
            package_id=row.id, name=row.name, canonical_package_id=None, url=url
        )

        # add url to map
        if url not in url_map:
            url_map[url] = []

        # add the package to the url map
        url_map[url].append(pkg)

    logger.log(f"Distinct URLs: {len(url_map)}")

    # generate canonical package ids for each url
    canonical_packages: dict[Canon, list[CanonPackage]] = {}
    for url, packages in url_map.items():
        # the first package is the canonical package
        deduped = packages[0]

        # set its id
        deduped.canonical_package_id = uuid4()

        # create the canon
        canon = Canon(id=deduped.canonical_package_id, name=deduped.name, url=url)

        # set the canonical package for the url
        canon_packages = generate_mapping(canon.id, packages)
        canonical_packages[canon] = canon_packages

    logger.log(f"Canonical packages: {len(canonical_packages)}")

    # load the canons and the mappings
    if LOAD:
        db.load_canonical_packages(list(canonical_packages.keys()))
        # the mappings need to be flattened first
        db.load_canonical_package_mappings(
            [item for sublist in canonical_packages.values() for item in sublist]
        )


def main():
    db = GraphDB()
    if db.is_canon_populated() or db.is_canon_package_populated():
        logger.warn(
            "Deduplicated graph already exists. Clear them to generate a new one."
        )
        exit(1)

    dedupe(db)


if __name__ == "__main__":
    main()
