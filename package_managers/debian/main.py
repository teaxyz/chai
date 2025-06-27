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
from core.structs import Cache, DiffResult, URLKey
from core.utils import file_exists
from package_managers.debian.db import DebianDB
from package_managers.debian.debian_sources import (
    build_package_to_source_mapping,
    enrich_package_with_source,
)
from package_managers.debian.diff import DebianDiff
from package_managers.debian.parser import DebianData, DebianParser

SCHEDULER_ENABLED = os.getenv("ENABLE_SCHEDULER", "true").lower() == "true"


def fetch(config: Config, logger: Logger) -> tuple[GZipFetcher, GZipFetcher]:
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


def diff(
    data: list[DebianData], config: Config, cache: Cache, db: DebianDB, logger: Logger
) -> DiffResult:
    # Keeps track of all the new packages we're adding
    seen: dict[str, UUID] = {}
    seen_new_pkg_urls: set[tuple[UUID, UUID]] = set()

    # Objects that we will return
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
    for i, debian_data in enumerate(data):
        import_id = f"debian/{debian_data.package}"
        if not import_id:
            logger.warn(f"Skipping package with empty name at index {i}")
            continue

        # Diff the package
        pkg_id, pkg_obj, update_payload = diff.diff_pkg(import_id, debian_data)

        # Guard: if pkg_obj is not None, that means it's a new package
        # If it's new, **and** we have seen it before, set the ID to what is seen
        # So, duplicates absorb all URLs & Dependencies under one umbrella
        resolved_pkg_id = seen.get(pkg_obj.import_id, pkg_id) if pkg_obj else pkg_id

        if pkg_obj and pkg_obj.import_id not in seen:
            logger.debug(f"New package: {pkg_obj.name}")
            new_packages.append(pkg_obj)
            seen[pkg_obj.import_id] = resolved_pkg_id
        if update_payload:
            logger.debug(f"Updated package: {update_payload['id']}")
            updated_packages.append(update_payload)

        # Diff URLs (resolved_urls is map of url types to final URL ID)
        resolved_urls = diff.diff_url(import_id, debian_data, new_urls)

        # Diff package URLs
        new_links, updated_links = diff.diff_pkg_url(resolved_pkg_id, resolved_urls)
        if new_links:
            logger.debug(f"New package URLs: {len(new_links)}")

            # guard: only add truly new links
            for link in new_links:
                if (link.package_id, link.url_id) not in seen_new_pkg_urls:
                    new_package_urls.append(link)
                    seen_new_pkg_urls.add((link.package_id, link.url_id))

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

        # In test mode, limit processing to the first 3 packages to reduce runtime and resource usage.
        if config.exec_config.test and i > 2:
            break

    return DiffResult(
        new_packages,
        new_urls,
        new_package_urls,
        updated_packages,
        updated_package_urls,
        new_deps,
        removed_deps,
    )


def run_pipeline(config: Config, db: DebianDB, logger: Logger):
    """The Debian Indexer"""

    package_fetcher, sources_fetcher = fetch(config, logger)
    input_dir = f"{sources_fetcher.output}/latest"

    # Build package-to-source mapping first
    sources_file_path = file_exists(input_dir, "sources")
    source_mapping = build_package_to_source_mapping(sources_file_path, logger)

    # Parse packages file
    packages_file_path = file_exists(input_dir, "packages")
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
    cache = Cache(
        db.graph.package_map,
        db.urls.url_map,
        db.urls.package_urls,
        db.graph.dependencies,
    )
    logger.log("Setup cache")

    # Perform the diff
    result = diff(enriched_packages, config, cache, db, logger)

    # Ingest all diffs
    db.ingest_wrapper(result)

    if config.exec_config.no_cache:
        package_fetcher.cleanup()
        sources_fetcher.cleanup()


def main(config: Config, db: DebianDB, logger: Logger):
    logger.log("Initializing Debian package manager")
    logger.debug(f"Config: {config}")

    if SCHEDULER_ENABLED:
        logger.log("Scheduler enabled. Starting schedule.")
        scheduler = Scheduler("debian_scheduler")
        scheduler.start(run_pipeline, config, db, logger)

        # run immediately as well when scheduling
        scheduler.run_now(run_pipeline, config, db, logger)

        # keep the main thread alive for scheduler
        try:
            while True:
                time.sleep(3600)
        except KeyboardInterrupt:
            scheduler.stop()
            logger.log("Scheduler stopped.")
    else:
        logger.log("Scheduler disabled. Running pipeline once.")
        run_pipeline(config, db, logger)
        logger.log("Pipeline finished.")


if __name__ == "__main__":
    config = Config(PackageManager.DEBIAN)
    db = DebianDB("debian_db", config)
    logger = Logger("debian")
    main(config, db, logger)
