import time

from core.config import Config, PackageManager
from core.db import DB
from core.fetcher import TarballFetcher
from core.logger import Logger
from core.scheduler import Scheduler
from package_managers.crates.transformer import CratesTransformer

logger = Logger("crates_orchestrator")


def fetch(config: Config) -> TarballFetcher:
    fetcher = TarballFetcher("crates", config)
    files = fetcher.fetch()
    fetcher.write(files)
    return fetcher


def load(db: DB, transformer: CratesTransformer, config: Config) -> None:
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


def run_pipeline(db: DB, config: Config) -> None:
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
    db = DB()
    config = Config(PackageManager.CRATES, db)
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
