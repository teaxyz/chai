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
    
    # Process data in streaming fashion
    logger.log("\n🔄 Starting streaming pipeline...")
    batch_num = 1
    total_processed = 0
    current_batch = []
    
    for data in fetch(config):
        try:
            packages = json.loads(data.content)
            logger.log(f"Processing batch {batch_num} with {len(packages)} packages")
            
            # Transform packages
            transformed_packages = []
            for package_data in packages:
                try:
                    info = package_data["info"]
                    package = {
                        "name": info["name"],
                        "import_id": info["name"],
                        "readme": info.get("description", ""),
                    }
                    transformed_packages.append(package)
                except (KeyError, TypeError) as e:
                    logger.error(f"Error transforming package: {e}")
                    continue
            
            # Insert batch into database
            if transformed_packages:
                logger.log(f"Inserting batch {batch_num} ({len(transformed_packages)} packages)")
                db.insert_packages(
                    iter(transformed_packages),
                    config.pm_config.pm_id,
                    PackageManager.PYPI.value,
                )
                total_processed += len(transformed_packages)
                logger.log(f"Total packages processed: {total_processed}")
            
            batch_num += 1
            
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding batch {batch_num}: {e}")
            continue
    
    logger.log(f"✅ Processed {total_processed} packages in {batch_num - 1} batches")
    
    # Process other data types
    if not config.exec_config.test:
        logger.log("\n🔄 Processing additional data...")
        transformer = PyPITransformer(config.url_types, config.user_types, [])  # Empty data since we've already processed packages
        
        logger.log("🌐 Loading URLs...")
        db.insert_urls(transformer.urls())
        
        logger.log("📎 Loading package URLs...")
        db.insert_package_urls(transformer.package_urls())
        
        logger.log("📋 Loading versions...")
        db.insert_versions(transformer.versions())
        
        logger.log("👥 Loading user versions...")
        db.insert_user_versions(transformer.user_versions(), config.user_types.github)
        
        logger.log("🔄 Loading dependencies...")
        db.insert_dependencies(transformer.dependencies())
    else:
        logger.log("ℹ️  Skipping detailed data in test mode")

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
