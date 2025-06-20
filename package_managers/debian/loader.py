from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from core.config import Config
from core.db import DB
from core.models import DependsOn, Package, Version
from package_managers.debian.transformer import Cache, TempDependency, TempVersion

BATCH_SIZE = 10000


class DebianLoader(DB):
    def __init__(self, config: Config, data: dict[str, Cache]):
        super().__init__("debian_db")
        self.data: dict[str, Cache] = data
        self.debug: bool = config.exec_config.test
        self.logger.debug(f"Initialized DebianLoader with {len(data)} cache entries")

    def load_packages(self) -> None:
        """
        Efficiently load all unique packages from the cache map into the database
        using bulk insertion and returning inserted IDs.
        """
        # Extract unique packages from the cache map
        unique_packages = {}
        for key, cache in self.data.items():
            package = cache.package
            # Validate that each package is a Package object
            if not isinstance(package, Package):
                self.logger.error(
                    f"Invalid package object for key {key}: {type(package)}"
                )
                continue

            if package.derived_id not in unique_packages:
                unique_packages[package.derived_id] = package

        self.logger.log(f"Found {len(unique_packages)} unique packages to insert")

        # Convert packages to dicts for bulk insertion
        package_dicts = []
        for pkg in unique_packages.values():
            try:
                package_dicts.append(pkg.to_dict())
            except Exception as e:
                self.logger.error(f"Error in to_dict for package {pkg.name}: {e!s}")

        if not package_dicts:
            self.logger.log("No packages to insert")
            return

        # Bulk insert packages with RETURNING clause to get IDs
        with self.session() as session:
            try:
                # Use the PostgreSQL dialect's insert function which supports
                # on_conflict_do_nothing
                stmt = pg_insert(Package).values(package_dicts).on_conflict_do_nothing()

                # Add returning clause
                stmt = stmt.returning(Package.id, Package.derived_id)

                self.logger.log("About to execute insert statement")

                # Execute and get results
                result = session.execute(stmt)
                inserted_packages = {row.derived_id: row.id for row in result}
                session.commit()

                self.logger.log(
                    f"Successfully inserted {len(inserted_packages)} packages"
                )

                # For packages that weren't inserted due to conflicts, fetch their IDs
                missing_derived_ids = [
                    derived_id
                    for derived_id in unique_packages
                    if derived_id not in inserted_packages
                ]

                self.logger.log(
                    f"Fetching {len(missing_derived_ids)} IDs for conflicting packages"
                )

                if missing_derived_ids:
                    stmt = select(Package.id, Package.derived_id).where(
                        Package.derived_id.in_(missing_derived_ids)
                    )
                    result = session.execute(stmt)
                    for row in result:
                        inserted_packages[row.derived_id] = row.id

                # Update all package objects in the cache with their IDs
                updated_count = 0
                for cache in self.data.values():
                    if cache.package.derived_id in inserted_packages:
                        cache.package.id = inserted_packages[cache.package.derived_id]
                        updated_count += 1

                self.logger.log(f"Updated cache with IDs for {updated_count} records")

            except Exception as e:
                self.logger.error(f"Error inserting packages: {e!s}")
                self.logger.error(f"Error type: {type(e)}")
                raise

    def load_versions(self) -> None:
        """
        Load all versions in the cache map into the database
        using bulk insertion and updating the cache with version IDs.

        This leverages the package IDs we've already cached during load_packages.
        """
        self.logger.log("Starting to load versions")

        # Extract all versions from the cache
        version_objects = []

        for key, cache in self.data.items():
            # Skip if package has no ID (shouldn't happen if load_packages was called)
            if not hasattr(cache.package, "id") or cache.package.id is None:
                raise ValueError(f"Package {key} has no ID when loading versions")

            for temp_version in cache.versions:
                # Check if this is a TempVersion that needs conversion
                if isinstance(temp_version, TempVersion):
                    # Convert TempVersion to proper Version with package_id
                    version = Version(
                        package_id=cache.package.id,
                        version=temp_version.version,
                        import_id=temp_version.import_id,
                    )
                    version_objects.append(version)
                # Otherwise, it's already a Version object
                elif isinstance(temp_version, Version):
                    # Ensure the version has the correct package_id
                    if (
                        not hasattr(temp_version, "package_id")
                        or temp_version.package_id is None
                    ):
                        temp_version.package_id = cache.package.id
                    version_objects.append(temp_version)
                else:
                    raise ValueError(f"Unexpected version type: {type(temp_version)}")

        self.logger.log(f"Found {len(version_objects)} versions to insert")

        if not version_objects:
            self.logger.log("No versions to insert")
            return

        # Convert versions to dicts for bulk insertion
        version_dicts = []
        for version in version_objects:
            try:
                version_dicts.append(version.to_dict())
            except Exception as e:
                self.logger.error(f"Error converting version to dict: {e!s}")
                raise e

        # Use batch processing for better performance
        self.logger.log(f"Using batch size of {BATCH_SIZE} for version insertion")

        version_id_map = {}  # Maps import_id to version id

        with self.session() as session:
            try:
                # Process versions in batches
                for i in range(0, len(version_dicts), BATCH_SIZE):
                    batch = version_dicts[i : i + BATCH_SIZE]
                    self.logger.log(
                        f"Processing batch {i // BATCH_SIZE + 1}/{(len(version_dicts) - 1) // BATCH_SIZE + 1} ({len(batch)} versions)"
                    )

                    # Use PostgreSQL dialect insert with returning clause
                    stmt = (
                        pg_insert(Version)
                        .values(batch)
                        .on_conflict_do_nothing()
                        .returning(Version.id, Version.import_id)
                    )

                    result = session.execute(stmt)
                    for row in result:
                        version_id_map[row.import_id] = row.id

                    self.logger.log(f"Inserted {len(batch)} versions in current batch")

                session.commit()
                self.logger.log(
                    f"Successfully inserted versions, got {len(version_id_map)} IDs"
                )

                # Get IDs for versions that already existed
                missing_import_ids = [
                    v.import_id
                    for v in version_objects
                    if v.import_id not in version_id_map
                ]

                if missing_import_ids:
                    self.logger.log(
                        f"Fetching IDs for {len(missing_import_ids)} existing versions"
                    )

                    # Process in batches to avoid overly large queries
                    for i in range(0, len(missing_import_ids), BATCH_SIZE):
                        batch = missing_import_ids[i : i + BATCH_SIZE]
                        stmt = select(Version.id, Version.import_id).where(
                            Version.import_id.in_(batch)
                        )
                        result = session.execute(stmt)

                        for row in result:
                            version_id_map[row.import_id] = row.id

                # Update the cache with version IDs
                updated_count = 0

                for cache in self.data.values():
                    for i, version in enumerate(cache.versions):
                        import_id = version.import_id

                        if import_id in version_id_map:
                            # Replace TempVersion with Version
                            if isinstance(version, TempVersion):
                                cache.versions[i] = Version(
                                    id=version_id_map[import_id],
                                    package_id=cache.package.id,
                                    version=version.version,
                                    import_id=version.import_id,
                                )
                            # If it's already a Version, just update the id
                            elif isinstance(version, Version):
                                version.id = version_id_map[import_id]

                            updated_count += 1

                self.logger.log(f"Updated cache with IDs for {updated_count} versions")

            except Exception as e:
                self.logger.error(f"Error inserting versions: {e!s}")
                self.logger.error(f"Error type: {type(e)}")
                raise e

    def load_dependencies(self) -> None:
        """
        Load all dependencies in the cache map into the database.

        This requires both package IDs and version IDs to be already in the cache,
        so it should be called after load_packages and load_versions.
        """
        self.logger.log("Starting to load dependencies")

        # Extract all dependencies from the cache
        dependency_objects = []
        missing = set()

        for key, cache in self.data.items():
            # Skip if package has no ID
            if not hasattr(cache.package, "id") or cache.package.id is None:
                raise ValueError(f"Package {key} has no ID when loading dependencies")

            for temp_dep in cache.dependency:
                # Check if this is a TempDependency that needs conversion
                if isinstance(temp_dep, TempDependency):
                    # Find the version ID for this package
                    version_id = None
                    for version in cache.versions:
                        if hasattr(version, "id") and version.id is not None:
                            version_id = version.id
                            break

                    if version_id is None:
                        raise ValueError(f"Couldn't find version ID for package {key}")

                    # Find the dependency package ID
                    dependency_id = None
                    dep_cache = self.data.get(temp_dep.dependency_name)
                    if (
                        dep_cache
                        and hasattr(dep_cache.package, "id")
                        and dep_cache.package.id is not None
                    ):
                        dependency_id = dep_cache.package.id

                    if dependency_id is None:
                        # TODO: certain Debian packages don't have a `"Package:`
                        # in the Packages or Sources files, so we can't load them
                        missing.add(temp_dep.dependency_name)
                        continue

                    # Create the DependsOn object
                    dependency = DependsOn(
                        version_id=version_id,
                        dependency_id=dependency_id,
                        dependency_type_id=temp_dep.dependency_type_id,
                        semver_range=temp_dep.semver_range,
                    )
                    dependency_objects.append(dependency)
                # Otherwise, it should already be a DependsOn object
                elif isinstance(temp_dep, DependsOn):
                    dependency_objects.append(temp_dep)
                else:
                    self.logger.warn(f"Unexpected dependency type: {type(temp_dep)}")

        self.logger.log(f"Found {len(dependency_objects)} dependencies to insert")

        if missing:
            self.logger.warn(
                f"{len(missing)} pkgs are deps, but in Packages/Sources file"
            )
            self.logger.warn(f"Missing pkgs: {missing}")

        if not dependency_objects:
            self.logger.log("No dependencies to insert")
            return

        # Convert dependencies to dicts for bulk insertion
        dependency_dicts = []
        for dep in dependency_objects:
            try:
                dependency_dicts.append(dep.to_dict())
            except Exception as e:
                self.logger.error(f"Error converting dependency to dict: {e!s}")

        # Use batch processing for better performance
        self.logger.log(f"Using batch size of {BATCH_SIZE} for dependency insertion")

        with self.session() as session:
            try:
                # Process dependencies in batches
                for i in range(0, len(dependency_dicts), BATCH_SIZE):
                    batch = dependency_dicts[i : i + BATCH_SIZE]
                    self.logger.log(
                        f"Processing batch {i // BATCH_SIZE + 1}/{(len(dependency_dicts) - 1) // BATCH_SIZE + 1} ({len(batch)} dependencies)"
                    )

                    # Use PostgreSQL dialect insert
                    stmt = pg_insert(DependsOn).values(batch).on_conflict_do_nothing()
                    session.execute(stmt)

                    self.logger.log(
                        f"Inserted {len(batch)} dependencies in current batch"
                    )

                session.commit()
                self.logger.log("Successfully inserted all dependencies")

            except Exception as e:
                self.logger.error(f"Error inserting dependencies: {e!s}")
                self.logger.error(f"Error type: {type(e)}")
                raise
