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
    
    for data in fetch(config):
        try:
            packages = json.loads(data.content)
            logger.log(f"Processing batch {batch_num} with {len(packages)} packages")
            
            # Transform packages and related data
            transformed_packages = []
            transformed_versions = []
            transformed_urls = []
            transformed_package_urls = []
            transformed_licenses = set()  # Use set to deduplicate licenses
            transformed_dependencies = []
            transformed_users = set()  # Use set to deduplicate users
            
            for package_data in packages:
                try:
                    info = package_data["info"]
                    name = info["name"]
                    
                    # Basic package info
                    package = {
                        "name": name,
                        "import_id": name,  # Just use the name as import_id for packages
                        "readme": info.get("description", ""),  # Use description as readme
                        "documentation": info.get("documentation"),
                        "repository": info.get("repository"),
                        "homepage": info.get("homepage")
                    }
                    transformed_packages.append(package)
                    
                    # Insert package first to get its ID
                    package_id = db.insert_packages(
                        iter([package]),
                        config.pm_config.pm_id,
                        PackageManager.PYPI.value,
                    )
                    
                    # License
                    license_id = None
                    license_name = info.get("license")
                    if license_name:
                        transformed_licenses.add(license_name)
                        license_id = db.select_license_by_name(license_name, create=True)
                    
                    # URLs
                    urls_to_add = []
                    if info.get("home_page"):
                        urls_to_add.append(("homepage", info["home_page"]))
                    if info.get("package_url"):
                        urls_to_add.append(("package", info["package_url"]))
                    if info.get("project_urls"):
                        for url_type, url in info["project_urls"].items():
                            urls_to_add.append((url_type.lower(), url))
                    
                    for url_type, url in urls_to_add:
                        # Get or create URL type
                        url_type_id = db.select_url_type(url_type, create=True).id
                        
                        # Check if URL exists
                        url_obj = db.select_url_by_url_and_type(url, url_type_id)
                        if not url_obj:
                            # Insert URL and get its ID
                            url_id = db.insert_urls(iter([{
                                "url": url,
                                "url_type_id": url_type_id
                            }]))
                        else:
                            url_id = url_obj.id
                        
                        # Create package URL relationship
                        if url_id and package_id:
                            transformed_package_urls.append({
                                "package_id": package_id,
                                "url_id": url_id
                            })
                    
                    # Versions and Dependencies
                    if "releases" in package_data:
                        for version, releases in package_data["releases"].items():
                            if releases:  # Only process versions that have actual releases
                                release = releases[0]  # Take first release file
                                version_data = {
                                    "package_id": package_id,
                                    "version": version,
                                    "import_id": name,  # Use package name as import_id
                                    "size": release.get("size"),
                                    "published_at": release.get("upload_time_iso_8601"),
                                    "license_id": license_id,
                                    "license": license_name,  # Add this for cache updates
                                    "downloads": release.get("downloads", 0),
                                    "checksum": release.get("digests", {}).get("sha256")
                                }
                                
                                # Only add version if we have a valid package_id
                                if package_id:
                                    transformed_versions.append(version_data)
                    
                    # Dependencies
                    if info.get("requires_dist"):
                        for req in info["requires_dist"]:
                            if req and "; extra ==" not in req:  # Skip optional dependencies
                                parts = req.split(" ", 1)
                                dep_name = parts[0]
                                version_constraint = parts[1] if len(parts) > 1 else ""
                                
                                # Get or create dependency type
                                dep_type_id = db.select_dependency_type_by_name("runtime", create=True).id
                                
                                # Get or create dependency package
                                dep_package = {
                                    "name": dep_name,
                                    "import_id": dep_name,
                                    "readme": ""  # Empty readme for dependencies
                                }
                                dep_package_id = db.insert_packages(
                                    iter([dep_package]),
                                    config.pm_config.pm_id,
                                    PackageManager.PYPI.value,
                                )
                                
                                transformed_dependencies.append({
                                    "version_id": None,  # Will be set after version insertion
                                    "dependency_id": dep_package_id,
                                    "dependency_type_id": dep_type_id,
                                    "semver_range": version_constraint,
                                    "import_id": dep_name  # Add import_id for version lookup
                                })
                    
                    # Users (author and maintainer)
                    if info.get("author"):
                        transformed_users.add((info["author"], info.get("author_email", "")))
                    if info.get("maintainer"):
                        transformed_users.add((info["maintainer"], info.get("maintainer_email", "")))
                    
                except (KeyError, TypeError) as e:
                    logger.error(f"Error transforming package: {e}")
                    continue
            
            # Insert remaining transformed data
            if transformed_package_urls:
                logger.log(f"Inserting {len(transformed_package_urls)} package URLs")
                db.insert_package_urls(iter(transformed_package_urls))
            
            if transformed_versions:
                logger.log(f"Inserting {len(transformed_versions)} versions")
                db.insert_versions(iter(transformed_versions))
                
                # Update version cache for dependencies
                db.update_caches(transformed_versions, update_versions=True)
            
            if transformed_dependencies:
                # Get version IDs from cache and filter out dependencies without version IDs
                valid_dependencies = []
                for dep in transformed_dependencies:
                    version_id = db.version_cache.get(dep["import_id"])
                    if version_id:
                        dep["version_id"] = version_id
                        valid_dependencies.append(dep)
                
                if valid_dependencies:
                    logger.log(f"Inserting {len(valid_dependencies)} dependencies")
                    db.insert_dependencies(iter(valid_dependencies))
            
            if transformed_users:
                logger.log(f"Inserting {len(transformed_users)} users")
                users_to_insert = [
                    {
                        "username": username,
                        "email": email,
                        "import_id": username  # Use username as import_id
                    }
                    for username, email in transformed_users
                ]
                db.insert_users(iter(users_to_insert), config.user_types.github)
            
            total_processed += len(transformed_packages)
            logger.log(f"Total packages processed: {total_processed}")
            
            batch_num += 1
            
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding batch {batch_num}: {e}")
            continue
    
    logger.log(f"✅ Processed {total_processed} packages in {batch_num - 1} batches")
    
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
