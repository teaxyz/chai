#!/usr/bin/env pkgx +python@3.11 uv run
import os
import sys
import time

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from core.config import Config, PackageManager
from core.fetcher import TarballFetcher
from core.logger import Logger
from core.scheduler import Scheduler
from package_managers.crates.db import CratesDB
from package_managers.crates.transformer import CratesTransformer

logger = Logger("crates_orchestrator")

SCHEDULER_ENABLED = os.getenv("ENABLE_SCHEDULER", "true").lower() == "true"


def fetch(config: Config) -> TarballFetcher:
    fetcher = TarballFetcher(
        "crates",
        config.pm_config.source,
        config.exec_config.no_cache,
        config.exec_config.test,
    )
    files = fetcher.fetch()
    fetcher.write(files)
    return fetcher


def load(db: CratesDB, transformer: CratesTransformer, config: Config) -> None:
    db.insert_packages(
        transformer.packages(),
        config.pm_config.pm_id,
        PackageManager.CRATES.value,
    )
    db.insert_users(transformer.users(), config.user_types.github)
    db.insert_user_packages(transformer.user_packages())

    if not config.exec_config.test:
        db.insert_urls(transformer.urls())
        db.insert_package_urls(transformer.package_urls())
        db.insert_versions(transformer.versions())
        db.insert_user_versions(transformer.user_versions(), config.user_types.github)
        db.insert_dependencies(transformer.dependencies())

    db.insert_load_history(config.pm_config.pm_id)
    logger.log("✅ crates")


def run_pipeline(db: CratesDB, config: Config) -> None:
    fetcher = fetch(config)
    transformer = CratesTransformer(config.url_types, config.user_types)
    load(db, transformer, config)
    fetcher.cleanup()

    coda = (
        "validate by running "
        + '`psql "postgresql://postgres:s3cr3t@localhost:5435/chai" '
        + '-c "SELECT * FROM load_history;"`'
    )
    logger.log(coda)


def main():
    db = CratesDB("crates_db")
    config = Config(PackageManager.CRATES)
    logger.debug(config)

    if SCHEDULER_ENABLED:
        logger.log("Scheduler enabled. Starting schedule.")
        scheduler = Scheduler("crates")
        scheduler.start(run_pipeline, db, config)

        # run immediately as well when scheduling
        scheduler.run_now(run_pipeline, db, config)

        # keep the main thread alive for scheduler
        try:
            while True:
                time.sleep(3600)
        except KeyboardInterrupt:
            scheduler.stop()
            logger.log("Scheduler stopped.")
    else:
        logger.log("Scheduler disabled. Running pipeline once.")
        run_pipeline(db, config)
        logger.log("Pipeline finished.")


if __name__ == "__main__":
    main()
