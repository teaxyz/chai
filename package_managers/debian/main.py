#!/usr/bin/env pkgx uv run

import os
import time
from datetime import datetime
from uuid import UUID

from core.config import Config, PackageManager
from core.fetcher import GZipFetcher
from core.logger import Logger
from core.models import URL, LegacyDependency, Package, PackageURL
from core.scheduler import Scheduler
from core.structs import Cache, URLKey
from package_managers.debian.db import DebianDB
from package_managers.debian.debian_sources import (
    build_package_to_source_mapping,
    enrich_package_with_source,
)
from package_managers.debian.diff import DebianDiff
from package_managers.debian.parser import DebianData, DebianParser

logger = Logger("debian")

SCHEDULER_ENABLED = os.getenv("ENABLE_SCHEDULER", "true").lower() == "true"
BATCH_SIZE = 500


def fetch(config: Config) -> tuple[GZipFetcher, GZipFetcher]:
    """Fetches the Debian packages & sources manifest files"""
    package_source = config.pm_config.source[0]
    sources_source = config.pm_config.source[1]
    no_cache = config.exec_config.no_cache
    test = config.exec_config.test

    package_fetcher = GZipFetcher(
        name="debian",
        source=package_source,
        no_cache=no_cache,
        test=test,
        file_path="",  # will autosave in data/debian/latest
        file_name="packages",
    )

    sources_fetcher = GZipFetcher(
        name="debian",
        source=sources_source,
        no_cache=no_cache,
        test=test,
        file_path="",  # will autosave in data/debian/latest
        file_name="sources",
    )

    # Fetch
    should_fetch = config.exec_config.fetch
    if should_fetch:
        package_files = package_fetcher.fetch()
        package_fetcher.write(package_files)
        logger.log(f"Fetched {len(package_files)} package files")

        sources_files = sources_fetcher.fetch()
        sources_fetcher.write(sources_files)
        logger.log(f"Fetched {len(sources_files)} sources files")

    return package_fetcher, sources_fetcher


def run_pipeline(config: Config, db: DebianDB):
    """A diff-based approach to loading debian data into CHAI"""

    package_fetcher, sources_fetcher = fetch(config)
    input_dir = f"{sources_fetcher.output}/latest"

    # Build package-to-source mapping first
    sources_file_path = os.path.join(input_dir, "sources")
    if not os.path.exists(sources_file_path):
        logger.error(f"Sources file not found at {sources_file_path}")
        return
    source_mapping = build_package_to_source_mapping(sources_file_path, logger)

    # Parse packages file
    packages_file_path = os.path.join(input_dir, "packages")
    if not os.path.exists(packages_file_path):
        logger.error(f"Packages file not found at {packages_file_path}")
        return

    with open(packages_file_path) as f:
        packages_content = f.read()
    packages_parser = DebianParser(packages_content)

    # Process each package and enrich with source information
    enriched_packages: list[DebianData] = []
    for package_data in packages_parser.parse():
        enriched_package = enrich_package_with_source(
            package_data, source_mapping, logger
        )
        enriched_packages.append(enriched_package)

    logger.log(f"Processed {len(enriched_packages)} enriched packages")

    # Grab all the URLs from enriched packages
    all_urls: set[str] = set()
    for package in enriched_packages:
        all_urls.add(package.homepage)
        all_urls.add(package.vcs_browser)
        all_urls.add(package.vcs_git)

    logger.log(f"Found {len(all_urls)} URLs to load")

    # Set up cache
    db.set_current_graph()
    db.set_current_urls(all_urls)
    logger.log("Set current URLs")

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
    diff = DebianDiff(config, cache, db, logger)

    # Process each enriched package
    for i, debian_data in enumerate(enriched_packages):
        logger.debug(f"Processing package {i}: {debian_data.package}")
        import_id = f"debian/{debian_data.package}"
        if not import_id:
            logger.warn(f"Skipping package with empty name at index {i}")
            continue

        # Diff the package
        pkg_id, pkg_obj, update_payload = diff.diff_pkg(import_id, debian_data)

        if pkg_obj:
            logger.debug(f"New package: {pkg_obj.name}")
            new_packages.append(pkg_obj)
        if update_payload:
            logger.debug(f"Updated package: {update_payload['id']}")
            updated_packages.append(update_payload)

        # Diff URLs (resolved_urls is map of url types to final URL ID)
        resolved_urls = diff.diff_url(import_id, debian_data, new_urls)

        # Diff package URLs
        new_links, updated_links = diff.diff_pkg_url(pkg_id, resolved_urls)
        if new_links:
            logger.debug(f"New package URLs: {len(new_links)}")
            new_package_urls.extend(new_links)
        if updated_links:
            updated_package_urls.extend(updated_links)

        # Diff dependencies
        new_dependencies, removed_dependencies = diff.diff_deps(import_id, debian_data)
        if new_dependencies:
            logger.debug(f"New dependencies: {len(new_dependencies)}")
            new_deps.extend(new_dependencies)
        if removed_dependencies:
            logger.debug(f"Removed dependencies: {len(removed_dependencies)}")
            removed_deps.extend(removed_dependencies)

        if config.exec_config.test and i > 2:
            break

    # Convert new_urls dict to list for ingestion
    final_new_urls = list(new_urls.values())

    # Ingest all diffs
    db.ingest(
        new_packages,
        final_new_urls,
        new_package_urls,
        new_deps,
        removed_deps,
        updated_packages,
        updated_package_urls,
    )

    if config.exec_config.no_cache:
        if package_fetcher:
            package_fetcher.cleanup()
        if sources_fetcher:
            sources_fetcher.cleanup()


def main(config: Config, db: DebianDB):
    logger.log("Initializing Debian package manager")
    logger.debug(f"Config: {config}")

    if SCHEDULER_ENABLED:
        logger.log("Scheduler enabled. Starting schedule.")
        scheduler = Scheduler("debian_scheduler")
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
    config = Config(PackageManager.DEBIAN)
    db = DebianDB("debian_db", config)
    main(config, db)
