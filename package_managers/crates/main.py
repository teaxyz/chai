import time

from core.config import Config, PackageManager, initialize
from core.db import DB
from core.fetcher import TarballFetcher
from core.logger import Logger
from core.scheduler import Scheduler
from package_managers.crates.transformer import CratesTransformer

logger = Logger("crates_orchestrator")
crates = PackageManager.CRATES


def fetch(config: Config) -> None:
    fetcher = TarballFetcher("crates", config.file_location)
    files = fetcher.fetch()
    fetcher.write(files)


def load(db: DB, transformer: CratesTransformer, config: Config) -> None:
    db.insert_packages(transformer.packages(), config.package_manager_id, "crates")
    db.insert_versions(transformer.versions())
    db.insert_users(transformer.users(), config.user_types.crates)
    db.insert_user_packages(transformer.user_packages())
    db.insert_urls(transformer.urls())

    if not config.test:
        # these are bigger files, so we skip them in tests
        db.insert_user_versions(transformer.user_versions(), config.user_types.github)
        # db.insert_package_urls(transformer.package_urls()) FIXME
        db.insert_dependencies(transformer.dependencies())

    db.insert_load_history(config.package_manager_id)
    logger.log("✅ crates")


def run_pipeline(db: DB, config: Config) -> None:
    if config.fetch:
        fetch(config)

    transformer = CratesTransformer(config.url_types, config.user_types)
    load(db, transformer, config)

    coda = (
        "validate by running "
        + '`psql "postgresql://postgres:s3cr3t@localhost:5435/chai" '
        + '-c "SELECT * FROM load_history;"`'
    )
    logger.log(coda)


def main():
    db = DB()
    config = initialize(crates, db)
    logger.debug(config)

    scheduler = Scheduler("crates")
    scheduler.start(run_pipeline, db, config)

    # run immediately
    scheduler.run_now(run_pipeline, db, config)

    # keep the main thread alive so we can terminate the program with Ctrl+C
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        scheduler.stop()


if __name__ == "__main__":
    main()
