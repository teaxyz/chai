from typing import Dict, List

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from core.config import Config
from core.db import DB
from core.models import (
    URL,
    LegacyDependency,
    Package,
    PackageURL,
)
from package_managers.pkgx.parser import DependencyBlock
from package_managers.pkgx.transformer import Cache

BATCH_SIZE = 10000


# NOTE: this is a separate instance of the db that is used in main
class PkgxLoader(DB):
    def __init__(self, config: Config, data: Dict[str, Cache]):
        super().__init__("pkgx_db")
        self.config = config
        self.data = data
        self.debug = config.exec_config.test
        self.logger.debug(f"Initialized PkgxLoader with {len(data)} packages")
        self.now = func.now()

    def load_packages(self) -> None:
        """
        Efficiently load all unique packages from the cache map into the database
        using bulk insertion and returning inserted IDs.
        """
        unique_packages = {}
        for key, cache in self.data.items():
            package = cache.package
            if not isinstance(package, Package):
                self.logger.error(
                    f"Invalid package object for key {key}: {type(package)}"
                )
                continue
            if package.derived_id not in unique_packages:
                unique_packages[package.derived_id] = package

        self.logger.log(f"Found {len(unique_packages)} unique packages to insert")

        package_dicts = []
        for pkg in unique_packages.values():
            try:
                package_dicts.append(pkg.to_dict())
            except Exception as e:
                self.logger.error(f"Error in to_dict for package {pkg.name}: {str(e)}")

        if not package_dicts:
            self.logger.log("No packages to insert")
            return

        with self.session() as session:
            try:
                stmt = pg_insert(Package).values(package_dicts).on_conflict_do_nothing()

                # TODO: can just generate the UUID myself and provide it, so no need to
                # return
                stmt = stmt.returning(Package.id, Package.derived_id)
                self.logger.log("About to execute insert statement for packages")
                result = session.execute(stmt)
                inserted_packages = {row.derived_id: row.id for row in result}
                session.commit()
                self.logger.log(
                    f"Successfully inserted {len(inserted_packages)} packages"
                )

                missing_derived_ids = [
                    derived_id
                    for derived_id in unique_packages.keys()
                    if derived_id not in inserted_packages
                ]
                self.logger.log(
                    f"Fetching {len(missing_derived_ids)} IDs for conflicting packages"
                )

                if missing_derived_ids:
                    # Fetch missing IDs in batches
                    for i in range(0, len(missing_derived_ids), BATCH_SIZE):
                        batch_ids = missing_derived_ids[i : i + BATCH_SIZE]
                        stmt = select(Package.id, Package.derived_id).where(
                            Package.derived_id.in_(batch_ids)
                        )
                        result = session.execute(stmt)
                        for row in result:
                            inserted_packages[row.derived_id] = row.id

                updated_count = 0
                for cache in self.data.values():
                    if cache.package.derived_id in inserted_packages:
                        cache.package.id = inserted_packages[cache.package.derived_id]
                        updated_count += 1
                self.logger.log(f"Updated cache with IDs for {updated_count} packages")

            except Exception as e:
                self.logger.error(f"Error inserting packages: {str(e)}")
                self.logger.error(f"Error type: {type(e)}")
                raise

    def load_urls(self) -> None:
        """
        Load all URLs in the cache map into the database.
        URLs have their own table and are linked to packages through a join table.
        This method should be called after load_packages to ensure packages have IDs.
        """
        self.logger.log("Starting to load URLs")

        url_objects: List[URL] = []
        package_id_map: Dict[str, List[str]] = {}  # Map of URLs to List of package IDs

        # for every package we've collected, grab the package ID
        # then, for every URL in the package, add it to the list of URLs
        # remember, we have to load package_urls, so we **always** need the package ID
        for key, cache in self.data.items():
            if not hasattr(cache.package, "id") or cache.package.id is None:
                self.logger.warn(f"Package {key} has no ID when loading URLs, skipping")
                continue

            package_id = cache.package.id

            for url in cache.urls:
                if not isinstance(url, URL):
                    self.logger.warn(f"Invalid URL object type: {type(url)}, skipping")
                    continue

                url_objects.append(url)
                if url.url not in package_id_map:
                    package_id_map[url.url] = []
                package_id_map[url.url].append(package_id)

        # collect the unique URLs
        unique_urls = {(url.url, url.url_type_id): url for url in url_objects}.values()
        self.logger.log(f"Found {len(unique_urls)} unique URLs to insert")

        if not unique_urls:
            self.logger.log("No URLs to insert")
            return

        url_dicts = []
        for url in unique_urls:
            try:
                # Exclude 'id' if it exists but is None, else SQLAlchemy might complain
                d = url.to_dict()
                if "id" in d and d["id"] is None:
                    del d["id"]
                url_dicts.append(d)
            except Exception as e:
                self.logger.error(f"Error converting URL to dict: {str(e)}")

        if not url_dicts:
            self.logger.log("No valid URL dicts to insert")
            return

        self.logger.log(f"Using batch size of {BATCH_SIZE} for URL insertion")
        url_id_map = {}  # Maps URL string to URL id

        with self.session() as session:
            try:
                for i in range(0, len(url_dicts), BATCH_SIZE):
                    batch = url_dicts[i : i + BATCH_SIZE]
                    self.logger.log(
                        f"Processing URL batch {i//BATCH_SIZE + 1}/{(len(url_dicts)-1)//BATCH_SIZE + 1} ({len(batch)} URLs)"  # noqa
                    )

                    stmt = (
                        pg_insert(URL)
                        .values(batch)
                        .on_conflict_do_nothing()
                        .returning(URL.id, URL.url)
                    )
                    result = session.execute(stmt)
                    for row in result:
                        url_id_map[row.url] = row.id
                    # Get the actual count of inserted rows
                    inserted_count = len(result.fetchall())
                    self.logger.log(f"Inserted {inserted_count} URLs in current batch")

                session.commit()

                missing_urls = [
                    u["url"] for u in url_dicts if u["url"] not in url_id_map
                ]

                if missing_urls:
                    self.logger.log(
                        f"Fetching IDs for {len(missing_urls)} existing URLs"
                    )
                    for i in range(0, len(missing_urls), BATCH_SIZE):
                        batch_urls = missing_urls[i : i + BATCH_SIZE]
                        stmt = select(URL.id, URL.url).where(URL.url.in_(batch_urls))
                        result = session.execute(stmt)
                        for row in result:
                            url_id_map[row.url] = row.id

                package_url_dicts = []
                for url_str, pkgs in package_id_map.items():
                    if url_str in url_id_map:
                        url_id = url_id_map[url_str]
                        for package_id in pkgs:
                            package_url_dicts.append(
                                {"package_id": package_id, "url_id": url_id}
                            )

                self.logger.log(
                    f"Found {len(package_url_dicts)} package-URL links to insert"
                )

                if package_url_dicts:
                    for i in range(0, len(package_url_dicts), BATCH_SIZE):
                        batch = package_url_dicts[i : i + BATCH_SIZE]
                        self.logger.log(
                            f"Processing PackageURL batch {i//BATCH_SIZE + 1}/{(len(package_url_dicts)-1)//BATCH_SIZE + 1} ({len(batch)} links)"  # noqa
                        )
                        stmt = (
                            pg_insert(PackageURL)
                            .values(batch)
                            .on_conflict_do_update(
                                constraint="uq_package_url",
                                set_=dict(updated_at=self.now),
                            )
                        )
                        session.execute(stmt)
                    session.commit()
                    self.logger.log("Successfully inserted all package-URL links")

                updated_count = 0
                for cache in self.data.values():
                    for url in cache.urls:
                        if url.url in url_id_map:
                            url.id = url_id_map[url.url]
                            updated_count += 1
                self.logger.log(
                    f"Updated cache with IDs for {updated_count} URL instances"
                )

            except Exception as e:
                self.logger.error(f"Error inserting URLs or PackageURLs: {str(e)}")
                self.logger.error(f"Error type: {type(e)}")
                raise

    def load_dependencies(self) -> None:
        """
        Load all dependencies into the LegacyDependency table.
        This requires package IDs to be loaded first.
        # FIXME: legacy dependencies are package to package relationships.
        # A migration is needed to move all dependencies to the LegacyDependency structure.
        """
        self.logger.log("Starting to load legacy dependencies")

        legacy_dependency_dicts = []
        missing = set()

        for key, cache in self.data.items():
            # Ensure the main package has an ID
            if not hasattr(cache.package, "id") or cache.package.id is None:
                self.logger.warn(
                    f"Package {key} has no ID when loading dependencies, skipping"
                )
                continue
            package_id = cache.package.id

            # Helper to process a list of dependency names for a given type
            def process_deps(dep_blocks: list[DependencyBlock], dep_type_id: str):
                for dep_block in dep_blocks:
                    # TODO: do we need to use this?
                    platform = dep_block.platform
                    for dep in dep_block.dependencies:
                        dep_name = dep.name
                        dep_semver = dep.semver

                        # Find the dependency package in our cache
                        dep_cache = self.data.get(dep_name)
                        if not dep_cache:
                            missing.add(dep_name)
                            continue

                        # Checks: has to have an ID
                        if (
                            not hasattr(dep_cache.package, "id")
                            or dep_cache.package.id is None
                        ):
                            self.logger.warn(
                                f"Dependency package '{dep_name}' has no ID, skipping linkage for '{key}'"  # noqa
                            )
                            continue
                        dependency_id = dep_cache.package.id

                        # Append data for bulk insert
                        legacy_dependency_dicts.append(
                            {
                                "package_id": package_id,
                                "dependency_id": dependency_id,
                                "dependency_type_id": dep_type_id,
                                "semver_range": dep_semver,
                            }
                        )

            # Process each dependency type
            process_deps(cache.dependencies.build, self.config.dependency_types.build)
            process_deps(cache.dependencies.test, self.config.dependency_types.test)
            process_deps(
                cache.dependencies.dependencies, self.config.dependency_types.runtime
            )

        self.logger.log(
            f"Found {len(legacy_dependency_dicts)} legacy dependencies to insert"
        )

        if missing:
            self.logger.warn(f"{len(missing)} pkgs are deps, but have no pkgx.yaml")
            self.logger.warn(f"Missing pkgs: {missing}")

        if not legacy_dependency_dicts:
            self.logger.log("No legacy dependencies to insert")
            return

        # Bulk insert legacy dependencies
        with self.session() as session:
            try:
                for i in range(0, len(legacy_dependency_dicts), BATCH_SIZE):
                    batch = legacy_dependency_dicts[i : i + BATCH_SIZE]
                    self.logger.log(
                        f"Processing LegacyDependency batch {i//BATCH_SIZE + 1}/{(len(legacy_dependency_dicts)-1)//BATCH_SIZE + 1} ({len(batch)} links)"  # noqa
                    )
                    stmt = (
                        pg_insert(LegacyDependency)
                        .values(batch)
                        .on_conflict_do_nothing()
                    )
                    session.execute(stmt)
                session.commit()
                self.logger.log("Successfully inserted all pkgx dependencies")

            except Exception as e:
                self.logger.error(f"Error inserting legacy dependencies: {str(e)}")
                self.logger.error(f"Error type: {type(e)}")
                raise
