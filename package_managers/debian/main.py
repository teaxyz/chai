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
from package_managers.debian.diff import DebianDiff
from package_managers.debian.parser import DebianData, DebianParser

logger = Logger("debian")

SCHEDULER_ENABLED = os.getenv("ENABLE_SCHEDULER", "true").lower() == "true"
BATCH_SIZE = 500


def fetch(config: Config) -> tuple[GZipFetcher, GZipFetcher]:
    should_fetch = config.exec_config.fetch
    if not should_fetch:
        logger.log("Fetching disabled, skipping fetch")
        return None, None

    logger.debug("Starting Debian package fetch")

    package_source = config.pm_config.source[0]
    sources_source = config.pm_config.source[1]
    no_cache = config.exec_config.no_cache
    test = config.exec_config.test

    package_fetcher = GZipFetcher(
        name="debian",
        source=package_source,
        no_cache=no_cache,
        test=test,
        file_path="",
        file_name="packages",
    )
    package_files = package_fetcher.fetch()
    logger.log(f"Fetched {len(package_files)} package files")
    package_fetcher.write(package_files)

    sources_fetcher = GZipFetcher(
        name="debian",
        source=sources_source,
        no_cache=no_cache,
        test=test,
        file_path="",  # just save it in data/debian/latest/
        file_name="sources",
    )
    sources_files = sources_fetcher.fetch()
    logger.log(f"Fetched {len(sources_files)} sources files")
    sources_fetcher.write(sources_files)

    return package_fetcher, sources_fetcher


def build_package_to_source_mapping(sources_file_path: str) -> dict[str, DebianData]:
    """
    Build a mapping from binary package names to their source information.

    Args:
        sources_file_path: Path to the sources file
        test: Whether to limit parsing for testing

    Returns:
        Dictionary mapping binary package names to source DebianData objects
    """
    logger.debug("Building package-to-source mapping")

    # Parse sources file
    with open(sources_file_path) as f:
        sources_content = f.read()
    sources_parser = DebianParser(sources_content)

    # Build mapping: binary_package_name -> source_debian_data
    package_to_source: dict[str, DebianData] = {}

    for source_data in sources_parser.parse():
        # Each source may produce multiple binary packages
        if source_data.binary:
            # Source has explicit binary list
            for binary_name in source_data.binary:
                binary_name = binary_name.strip()
                if binary_name:
                    package_to_source[binary_name] = source_data
        else:
            # No explicit binary list, assume source name == binary name
            if source_data.package:
                package_to_source[source_data.package] = source_data

    logger.log(
        f"Built mapping for {len(package_to_source)} binary packages from sources"
    )
    return package_to_source


def enrich_package_with_source(
    package_data: DebianData, source_mapping: dict[str, DebianData]
) -> DebianData:
    """
    Enrich a package with its corresponding source information.

    Args:
        package_data: The package data from packages file
        source_mapping: Mapping from package names to source data

    Returns:
        Enriched DebianData with both package and source information
    """
    # Start with the package data
    enriched = package_data

    # Determine source name
    binary_name = package_data.package

    # Look up source information
    if binary_name in source_mapping:
        source_data = source_mapping[binary_name]

        # Enrich package with source information
        # Only add source fields that aren't already populated
        if not enriched.vcs_browser and source_data.vcs_browser:
            enriched.vcs_browser = source_data.vcs_browser
        if not enriched.vcs_git and source_data.vcs_git:
            enriched.vcs_git = source_data.vcs_git
        if not enriched.directory and source_data.directory:
            enriched.directory = source_data.directory
        if not enriched.build_depends and source_data.build_depends:
            enriched.build_depends = source_data.build_depends
        if not enriched.homepage and source_data.homepage:
            enriched.homepage = source_data.homepage

    else:
        # Log warning for missing source
        source_name = package_data.source or package_data.package
        logger.warn(
            f"Binary '{binary_name}' of source '{source_name}' was not found in sources file"
        )

    return enriched


def run_pipeline(config: Config, db: DebianDB):
    """A diff-based approach to loading debian data into CHAI"""

    package_fetcher, sources_fetcher = fetch(config)

    input_dir = "data/debian/latest"

    # Build package-to-source mapping first
    sources_file_path = os.path.join(input_dir, "sources")
    if not os.path.exists(sources_file_path):
        logger.error(f"Sources file not found at {sources_file_path}")
        return

    source_mapping = build_package_to_source_mapping(sources_file_path)

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
        enriched_package = enrich_package_with_source(package_data, source_mapping)
        enriched_packages.append(enriched_package)

    logger.log(f"Processed {len(enriched_packages)} enriched packages")

    # Set up cache
    db.set_current_graph()
    db.set_current_urls()
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
        if package_fetcher:
            package_fetcher.cleanup()
        if sources_fetcher:
            sources_fetcher.cleanup()


def main():
    logger.log("Initializing Debian package manager")
    config = Config(PackageManager.DEBIAN)
    db = DebianDB("debian_db", config)
    logger.debug(f"Using config: {config}")

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
    main()
