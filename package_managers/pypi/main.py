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
    logger.log("\n🚀 Starting PyPI pipeline...")
    logger.log(f"Mode: {'TEST' if config.exec_config.test else 'PRODUCTION'}")
    logger.log(f"Fetch new data: {config.exec_config.fetch}")
    logger.log(f"Cache enabled: {not config.exec_config.no_cache}")
    
    # Just download and save data for now
    logger.log("\n🔄 Starting download pipeline...")
    for data in fetch(config):
        logger.log(f"Saved batch to {data.file_name}")


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
