#!/usr/bin/env pkgx +python@3.11 uv run

import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from core.config import Config, PackageManager
from core.fetcher import GitFetcher
from core.logger import Logger
from package_managers.pkgx.loader import PkgxLoader
from package_managers.pkgx.parser import PkgxParser
from package_managers.pkgx.transformer import PkgxTransformer

logger = Logger("pkgx")

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


def run_pipeline(config: Config):
    fetcher = fetch(config)
    output_dir = f"{fetcher.output}/latest"

    # now, we'll parse the package.yml files
    pkgx_parser = PkgxParser(output_dir)
    pkgx_transformer = PkgxTransformer(config)

    for data, id in pkgx_parser.parse_packages():
        pkgx_transformer.transform(id, data)

    logger.log(f"Loaded {len(pkgx_transformer.cache_map)} packages")

    pkgx_loader = PkgxLoader(config, pkgx_transformer.cache_map)
    pkgx_loader.load_packages()
    pkgx_loader.load_urls()
    pkgx_loader.load_dependencies()

    if config.exec_config.no_cache:
        fetcher.cleanup()


def main():
    logger.log("Initializing Pkgx package manager")
    config = Config(PackageManager.PKGX)
    logger.debug(f"Using config: {config}")

    run_pipeline(config)


if __name__ == "__main__":
    main()
