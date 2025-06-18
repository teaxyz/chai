#!/usr/bin/env pkgx +python@3.11 uv run

import os
import time
from datetime import datetime
from uuid import UUID

from core.config import Config, PackageManager
from core.fetcher import GitFetcher
from core.logger import Logger
from core.models import URL, LegacyDependency, Package, PackageURL
from core.scheduler import Scheduler
from core.structs import Cache, URLKey
from package_managers.pkgx.db import PkgxDB
from package_managers.pkgx.diff import PkgxDiff
from package_managers.pkgx.parser import PkgxParser

logger = Logger("pkgx")

SCHEDULER_ENABLED = os.getenv("ENABLE_SCHEDULER", "true").lower() == "true"
BATCH_SIZE = 500
PROJECTS_DIR = "projects"
PACKAGE_FILE = "package.yml"


def fetch(config: Config) -> GitFetcher:
    should_fetch = config.exec_config.fetch
    fetcher = GitFetcher(
        "pkgx",
        config.pm_config.source,
        config.exec_config.no_cache,
        config.exec_config.test,
    )

    if should_fetch:
        logger.debug("Starting Pkgx package fetch")
        fetcher.fetch()
    else:  # symlink would still be updated
        logger.log("Fetching disabled, skipping fetch")

    # if no_cache is on, we'll delete stuff from here
    return fetcher


def run_pipeline(config: Config, db: PkgxDB):
    """A diff-based approach to loading pkgx data into CHAI"""

    fetcher = fetch(config)
    output_dir = f"{fetcher.output}/latest"

    # Parse all packages
    pkgx_parser = PkgxParser(output_dir)
    packages = list(pkgx_parser.parse_packages())

    logger.log(f"Parsed {len(packages)} packages")

    # Set up cache
    db.set_current_graph()
    db.set_current_urls()
    logger.log("Set current URLs")

    # Build cache for differential loading
    cache = Cache(
        db.graph.package_map,
        db.urls.url_map,
        db.urls.package_urls,
        db.graph.dependencies,
    )

    # Initialize differential loading collections
    new_packages: list[Package] = []
    new_urls: dict[URLKey, URL] = {}
    new_package_urls: list[PackageURL] = []
    updated_packages: list[dict[str, UUID | str | datetime]] = []
    updated_package_urls: list[dict[str, UUID | datetime]] = []
    new_deps: list[LegacyDependency] = []
    removed_deps: list[LegacyDependency] = []

    # Create diff processor
    diff = PkgxDiff(config, cache, db, logger)

    # Process each package
    for i, (pkg_data, import_id) in enumerate(packages):
        # Diff the package
        pkg_id, pkg_obj, update_payload = diff.diff_pkg(import_id, pkg_data)

        if pkg_obj:
            logger.debug(f"New package: {pkg_obj.name}")
            new_packages.append(pkg_obj)
        if update_payload:
            logger.debug(f"Updated package: {update_payload['id']}")
            updated_packages.append(update_payload)

        # Diff URLs (resolved_urls is map of url types to final URL ID)
        resolved_urls = diff.diff_url(import_id, pkg_data, new_urls)

        # Diff package URLs
        new_links, updated_links = diff.diff_pkg_url(pkg_id, resolved_urls)
        if new_links:
            logger.debug(f"New package URLs: {len(new_links)}")
            new_package_urls.extend(new_links)
        if updated_links:
            updated_package_urls.extend(updated_links)

        # Diff dependencies
        new_dependencies, removed_dependencies = diff.diff_deps(import_id, pkg_data)
        if new_dependencies:
            logger.debug(f"New dependencies: {len(new_dependencies)}")
            new_deps.extend(new_dependencies)
        if removed_dependencies:
            logger.debug(f"Removed dependencies: {len(removed_dependencies)}")
            removed_deps.extend(removed_dependencies)

        if config.exec_config.test and i > 10:
            break

    # Convert new_urls dict to list for ingestion
    final_new_urls = list(new_urls.values())

    # Ingest all diffs
    db.ingest(
        new_packages,
        final_new_urls,
        new_package_urls,
        updated_packages,
        updated_package_urls,
        new_deps,
        removed_deps,
    )

    if config.exec_config.no_cache:
        fetcher.cleanup()


def main():
    logger.log("Initializing Pkgx package manager")
    config = Config(PackageManager.PKGX)
    db = PkgxDB("pkgx_main_db_logger", config)
    logger.debug(f"Using config: {config}")

    if SCHEDULER_ENABLED:
        logger.log("Scheduler enabled. Starting schedule.")
        scheduler = Scheduler("pkgx")
        scheduler.start(run_pipeline, config)

        # run immediately as well when scheduling
        scheduler.run_now(run_pipeline, config, db)

        # keep the main thread alive for scheduler
        try:
            while True:
                time.sleep(3600)
        except KeyboardInterrupt:
            scheduler.stop()
            logger.log("Scheduler stopped.")
    else:
        logger.log("Scheduler disabled. Running pipeline once.")
        run_pipeline(config, db)
        logger.log("Pipeline finished.")


if __name__ == "__main__":
    main()
