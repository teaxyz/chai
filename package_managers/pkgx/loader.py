from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from core.config import Config
from core.db import DB
from core.models import (
    LegacyDependency,
    Package,
)
from package_managers.pkgx.parser import DependencyBlock
from package_managers.pkgx.transformer import Cache

BATCH_SIZE = 10000


# NOTE: this is a separate instance of the db that is used in main
class PkgxLoader(DB):
    def __init__(self, config: Config, data: dict[str, Cache]):
        super().__init__("pkgx_db")
        self.config = config
        self.data = data
        self.debug = config.exec_config.test
        self.logger.debug(f"Initialized PkgxLoader with {len(data)} packages")

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
                self.logger.error(f"Error in to_dict for package {pkg.name}: {e!s}")

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
                    for derived_id in unique_packages
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
                self.logger.error(f"Error inserting packages: {e!s}")
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
            def process_deps(
                dep_blocks: list[DependencyBlock],
                dep_type_id: str,
                key=key,
                package_id=package_id,
            ):
                for dep_block in dep_blocks:
                    # TODO: do we need to use this?
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
                                f"Dependency package '{dep_name}' has no ID, skipping linkage for '{key}'"
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
                        f"Processing LegacyDependency batch {i//BATCH_SIZE + 1}/{(len(legacy_dependency_dicts)-1)//BATCH_SIZE + 1} ({len(batch)} links)"
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
                self.logger.error(f"Error inserting legacy dependencies: {e!s}")
                self.logger.error(f"Error type: {type(e)}")
                raise


if __name__ == "__main__":
    from core.config import PackageManager

    config = Config(PackageManager.PKGX)
    db = DB(config)
    loader = PkgxLoader(config, {})
    loader.load_urls_v2()
