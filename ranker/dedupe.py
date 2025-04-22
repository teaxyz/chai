#!/usr/bin/env uv run

from dataclasses import dataclass, field
from typing import Dict, List, Tuple
from uuid import UUID, uuid4

from core.logger import Logger
from core.models import Canon, CanonPackage
from core.utils import env_vars
from ranker.config import load_config
from ranker.db import GraphDB

LOAD = env_vars("LOAD", "false")
logger = Logger("dedupe_main")


@dataclass
class DedupedPackage:
    package_id: UUID
    name: str
    url: str
    canonical_package_id: UUID | None = None


def bad_homepage_url(url: str) -> bool:
    match url:
        case "null":  # from legacy data, a bunch of npm projects have "null"
            return True
        case "":
            return True
        case _:
            return False


def dedupe(db: GraphDB):
    # 1. Fetch Current State
    logger.log("1. Fetching existing canonical URLs...")
    current_canons: Dict[str, UUID] = db.get_all_canons()
    logger.log(f"Found {len(current_canons)} existing canonical URLs.")

    # 2. Calculate Desired State (based on latest URLs)
    logger.log("2. Fetching latest package homepage URLs...")
    package_url_data: List[Tuple[UUID, str, str, str]] = db.get_packages_with_urls()
    logger.log(f"Collected {len(package_url_data)} total homepage URL entries.")

    # Get the most recent Homepage URL per package
    latest_package_info: Dict[UUID, Tuple[str, str]] = {}
    for pkg_id, pkg_name, url, _ in package_url_data:
        if pkg_id not in latest_package_info:
            # Store only name and latest url per package_id
            latest_package_info[pkg_id] = (pkg_name, url)
    logger.log(f"Found {len(latest_package_info)} packages with latest homepage URLs.")

    # Build a map of desired URLs and the packages associated with them
    desired_url_to_packages: Dict[str, List[Tuple[UUID, str]]] = {}
    for pkg_id, (pkg_name, url) in latest_package_info.items():
        if bad_homepage_url(url):
            continue
        if url not in desired_url_to_packages:
            desired_url_to_packages[url] = []
        desired_url_to_packages[url].append((pkg_id, pkg_name))

    logger.log(f"Found {len(desired_url_to_packages)} distinct valid desired URLs.")

    # 3. Reconcile and Apply Changes

    # Identify New Canons and build complete URL -> Canon ID map
    canons_to_insert: List[Canon] = []
    final_url_to_canon_id: Dict[str, UUID] = current_canons.copy()

    logger.log("3. Identifying new canons and building complete URL -> Canon ID map...")
    for url, packages_info in desired_url_to_packages.items():
        if url not in final_url_to_canon_id:
            # This is a new canonical URL
            new_canon_id = uuid4()
            # Use the name of the first package found for this URL as the canonical name
            # TODO: Revisit this. Probably store all names in an aliases table
            canon_name = packages_info[0][1]
            canons_to_insert.append(Canon(id=new_canon_id, name=canon_name, url=url))
            final_url_to_canon_id[url] = new_canon_id
            logger.debug(f"Identified new canon for URL: {url} with ID: {new_canon_id}")

    logger.log(f"Identified {len(canons_to_insert)} new canonical URLs to insert.")

    # Prepare Mappings (desired state for all packages)
    mappings_to_load: List[CanonPackage] = []
    for pkg_id, (pkg_name, url) in latest_package_info.items():
        if bad_homepage_url(url):
            continue

        if url in final_url_to_canon_id:
            target_canon_id = final_url_to_canon_id[url]
            mappings_to_load.append(
                CanonPackage(id=uuid4(), canon_id=target_canon_id, package_id=pkg_id)
            )
        else:
            # This should not happen if logic above is correct, but log just in case
            logger.warn(
                f"URL '{url}' for package {pkg_id} ({pkg_name}) not in final canon map."
            )

    logger.log(
        f"Prepared {len(mappings_to_load)} canonical package mappings for loading."
    )

    # Load the Canons and the Mappings
    if LOAD:
        logger.log("Loading new canonical packages...")
        db.load_canonical_packages(canons_to_insert)
        logger.log("Loading/Updating canonical package mappings...")
        db.load_canonical_package_mappings(mappings_to_load)
        logger.log("Database load operations complete.")
    else:
        logger.log("LOAD=false, skipping database load operations.")

    logger.log("✅ Deduplication process finished.")


def main():
    config = load_config()
    db = GraphDB(config.pm_config.npm_pm_id, config.pm_config.system_pm_ids)
    dedupe(db)


if __name__ == "__main__":
    main()
