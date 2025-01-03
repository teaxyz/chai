import time

from core.config import Config, PackageManager
from core.db import DB
from core.logger import Logger
from core.scheduler import Scheduler
from package_managers.pypi.fetcher import PyPIFetcher
from package_managers.pypi.transformer import PyPITransformer

logger = Logger("pypi_orchestrator")


def fetch(config: Config) -> PyPIFetcher:
    logger.log("🔄 Starting PyPI data fetching process...")
    fetcher = PyPIFetcher("pypi", config)
    
    if config.exec_config.fetch:
        logger.log("📥 Fetching new data from PyPI...")
        files = fetcher.fetch()
        fetcher.write(files)
        logger.log("✅ Data fetching completed")
    else:
        logger.log("ℹ️  Skipping fetch (FETCH=false)")
    
    return fetcher


def load(db: DB, transformer: PyPITransformer, config: Config) -> None:
    logger.log("\n🔄 Starting data loading process...")
    
    logger.log("📦 Loading packages...")
    db.insert_packages(
        transformer.packages(),
        config.pm_config.pm_id,
        PackageManager.PYPI.value,
    )
    
    logger.log("👤 Loading users...")
    db.insert_users(transformer.users(), config.user_types.github)
    
    logger.log("🔗 Loading user-package relationships...")
    db.insert_user_packages(transformer.user_packages())

    if not config.exec_config.test:
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


def run_pipeline(db: DB, config: Config) -> None:
    logger.log("\n🚀 Starting PyPI pipeline...")
    logger.log(f"Mode: {'TEST' if config.exec_config.test else 'PRODUCTION'}")
    logger.log(f"Fetch new data: {config.exec_config.fetch}")
    logger.log(f"Cache enabled: {not config.exec_config.no_cache}")
    
    fetcher = fetch(config)
    
    logger.log("\n🔄 Initializing transformer...")
    transformer = PyPITransformer(config.url_types, config.user_types)
    
    load(db, transformer, config)
    
    if config.exec_config.no_cache:
        logger.log("\n🧹 Cleaning up temporary files...")
        fetcher.cleanup()
        logger.log("✅ Cleanup completed")

    logger.log("\n✨ Pipeline completed successfully!")
    logger.log("To validate the results, run:")
    logger.log('`psql "postgresql://postgres:s3cr3t@localhost:5435/chai" -c "SELECT * FROM load_history;"`')


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
