import time
import json
from typing import Generator

from core.config import Config, PackageManager
from core.db import DB
from core.logger import Logger
from core.scheduler import Scheduler
from package_managers.pypi.fetcher import PyPIFetcher, Data
from package_managers.pypi.transformer import PyPITransformer

logger = Logger("pypi_orchestrator")


def fetch(config: Config) -> Generator[Data, None, None]:
    logger.log("🔄 Starting PyPI data fetching process...")
    fetcher = PyPIFetcher("pypi", config)
    
    if config.exec_config.fetch:
        logger.log("📥 Fetching new data from PyPI...")
        return fetcher.fetch()
    else:
        logger.log("ℹ️  Skipping fetch (FETCH=false)")
        return iter([])  # Return empty iterator


def run_pipeline(db: DB, config: Config) -> None:
    """Run the PyPI pipeline."""
    logger.log("\n🚀 Starting PyPI pipeline...")
    logger.log(f"Mode: {'TEST' if config.exec_config.test else 'PRODUCTION'}")
    
    # Create transformer
    transformer = PyPITransformer(
        url_types=config.url_types,
        user_types=config.user_types,
        pm_config=config.pm_config,
        db=db
    )
    
    # Step 1: Insert packages and related data
    logger.log("\n📦 Inserting packages...")
    db.insert_packages(
        transformer.packages(),
        config.pm_config.pm_id,
        PackageManager.PYPI.value,
    )
    
    # Step 2: Insert licenses
    # This is handled during package insertion as we create licenses on demand
    
    # Step 3: Skip user-related operations as we can't get GitHub info from PyPI
    
    # Step 4: Insert URLs and package URLs
    logger.log("\n🔗 Inserting URLs...")
    db.insert_urls(transformer.urls())
    db.insert_package_urls(transformer.package_urls())
    
    # Step 5: Insert versions
    logger.log("\n📝 Inserting versions...")
    db.insert_versions(transformer.versions())
    
    # Step 6: Insert dependencies (after all packages are in)
    logger.log("\n🔄 Inserting dependencies...")
    db.insert_dependencies(transformer.dependencies())
    
    # Record load history
    db.insert_load_history(config.pm_config.pm_id)
    logger.log("✅ PyPI data loading completed successfully")


def main():
    logger.log("\n📦 Initializing PyPI Package Manager...")
    db = DB()
    config = Config(PackageManager.PYPI, db)
    logger.debug(f"Configuration: {config}")

    scheduler = Scheduler("pypi")
    scheduler.start(run_pipeline, db, config)

    # run immediately
    scheduler.run_now(run_pipeline, db, config)

    # keep the main thread alive so we can terminate the program with Ctrl+C
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.log("\n⚠️  Received interrupt signal, shutting down...")
        scheduler.stop()
        logger.log("✅ Shutdown complete")


if __name__ == "__main__":
    main()
