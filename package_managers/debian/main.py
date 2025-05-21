#!/usr/bin/env pkgx +python@3.11 uv run --with alembic==1.13.2 --with certifi==2024.8.30 --with charset-normalizer==3.3.2 --with idna==3.8 --with mako==1.3.5 --with markupsafe==2.1.5 --with psycopg2==2.9.9 --with pyyaml==6.0.2 --with requests==2.32.3 --with ruff==0.6.5 --with schedule==1.2.0 --with sqlalchemy==2.0.34 --with typing-extensions==4.12.2 --with urllib3==2.2.2 #noqa

import os
import sys
import time

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from core.config import Config, PackageManager
from core.fetcher import GZipFetcher
from core.logger import Logger
from core.scheduler import Scheduler
from package_managers.debian.loader import DebianLoader
from package_managers.debian.transformer import DebianTransformer

logger = Logger("debian")

SCHEDULER_ENABLED = os.getenv("ENABLE_SCHEDULER", "true").lower() == "true"


def fetch(config: Config) -> None:
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

    logger.log("Cleaning up fetcher")
    package_fetcher.cleanup()
    sources_fetcher.cleanup()


def run_pipeline(config: Config) -> None:
    logger.log("Starting Debian pipeline")
    fetch(config)
    transformer = DebianTransformer(config)
    transformer.transform()
    loader = DebianLoader(config, transformer.cache_map)

    # Use the optimized bulk loading methods
    logger.log("Loading packages...")
    loader.load_packages()

    logger.log("Loading versions...")
    loader.load_versions()

    logger.log("Loading dependencies...")
    loader.load_dependencies()

    logger.log("Loading URLs...")
    loader.load_urls(loader.data)

    logger.log("Pipeline completed")


def main():
    logger.log("Initializing Debian package manager")
    config = Config(PackageManager.DEBIAN)
    logger.debug(f"Using config: {config}")

    if SCHEDULER_ENABLED:
        logger.log("Scheduler enabled. Starting schedule.")
        scheduler = Scheduler("debian")
        scheduler.start(run_pipeline, config)

        # run immediately as well when scheduling
        scheduler.run_now(run_pipeline, config)

        # keep the main thread alive for scheduler
        try:
            while True:
                time.sleep(3600)
        except KeyboardInterrupt:
            scheduler.stop()
            logger.log("Scheduler stopped.")
    else:
        logger.log("Scheduler disabled. Running pipeline once.")
        run_pipeline(config)
        logger.log("Pipeline finished.")


if __name__ == "__main__":
    main()
