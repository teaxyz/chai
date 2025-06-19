#!/usr/bin/env pkgx +python@3.11 uv run --with alembic==1.13.2 --with certifi==2024.8.30 --with charset-normalizer==3.3.2 --with idna==3.8 --with mako==1.3.5 --with markupsafe==2.1.5 --with psycopg2==2.9.9 --with pyyaml==6.0.2 --with requests==2.32.3 --with ruff==0.6.5 --with schedule==1.2.0 --with sqlalchemy==2.0.34 --with typing-extensions==4.12.2 --with urllib3==2.2.2

import os
import sys
import time
from datetime import datetime
from uuid import UUID

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
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
        "debian", package_source, no_cache, test, "debian", "packages"
    )
    package_files = package_fetcher.fetch()
    logger.log(f"Fetched {len(package_files)} package files")
    package_fetcher.write(package_files)

    sources_fetcher = GZipFetcher(
        "debian", sources_source, no_cache, test, "debian", "sources"
    )
    sources_files = sources_fetcher.fetch()
    logger.log(f"Fetched {len(sources_files)} sources files")
    sources_fetcher.write(sources_files)

    return package_fetcher, sources_fetcher


def run_pipeline(config: Config, db: DebianDB):
    """A diff-based approach to loading debian data into CHAI"""

    package_fetcher, sources_fetcher = fetch(config)

    # Read and parse all packages
    packages: list[DebianData] = []
    input_dir = "data/debian/latest"

    # Parse packages file
    # if package_fetcher:
    #     # Read the written file using the same pattern as transformer
    #     packages_file_path = os.path.join(input_dir, "debian", "packages")
    #     with open(packages_file_path) as f:
    #         package_content = f.read()
    #     package_parser = DebianParser(package_content, config.exec_config.test)
    #     packages.extend(list(package_parser.parse()))

    # Parse sources file
    if sources_fetcher:
        # Read the written file using the same pattern as transformer
        sources_file_path = os.path.join(input_dir, "debian", "sources")
        with open(sources_file_path) as f:
            sources_content = f.read()
        sources_parser = DebianParser(sources_content, config.exec_config.test)
        packages.extend(list(sources_parser.parse()))

    logger.log(f"Parsed {len(packages)} total packages")

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

    # Process each package
    for i, debian_data in enumerate(packages):
        print("-" * 100)
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
    db = DebianDB("debian_main_db_logger", config)
    logger.debug(f"Using config: {config}")

    if SCHEDULER_ENABLED:
        logger.log("Scheduler enabled. Starting schedule.")
        scheduler = Scheduler("debian")
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
