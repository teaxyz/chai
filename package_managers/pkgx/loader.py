from dataclasses import dataclass
from typing import Dict, List, Set, Tuple
from uuid import UUID, uuid4

from sqlalchemy import select, update
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

    # NOTE: this is the cleanest function of the bunch. All the others, should follow
    # a similar approach of figuring out what updates need to be made, and only applying
    # those, rather than relying on the on_conflict_do_nothing.
    def load_urls(self) -> None:
        """
        Refactored load URLs to use a diff on the current / desired state to find new
        URLs and existing relationships to update.
        """

        @dataclass
        class CurrentURLs:
            url_map: Dict[Tuple[str, UUID], URL]  # URL and URL Type ID to URL object
            package_urls: Dict[UUID, Set[PackageURL]]  # Package ID to PackageURL rows

        @dataclass
        class DiffResult:
            new_urls: List[URL]
            new_package_urls: List[PackageURL]
            urls_to_update: List[PackageURL]

        def get_current_urls(urls: Set[str]) -> CurrentURLs:
            stmt = (
                select(Package, PackageURL, URL)
                .select_from(URL)
                .join(PackageURL, PackageURL.url_id == URL.id, isouter=True)
                .join(Package, Package.id == PackageURL.package_id, isouter=True)
                .where(URL.url.in_(urls))
            )

            with self.session() as session:
                result = session.execute(stmt)

                url_map: Dict[Tuple[str, UUID], URL] = {}
                package_urls: Dict[UUID, Set[PackageURL]] = {}

                for pkg, pkg_url, url in result:
                    url_map[(url.url, url.url_type_id)] = url

                    # since it's a left join, we need to check if pkg is None
                    if pkg is not None:
                        if pkg.id not in package_urls:
                            package_urls[pkg.id] = set()
                        package_urls[pkg.id].add(pkg_url)

                return CurrentURLs(url_map=url_map, package_urls=package_urls)

        def get_desired_state() -> Dict[UUID, Set[URL]]:
            """Based on the cache, return the map of package ID to URLs"""
            desired_state: Dict[UUID, Set[URL]] = {}
            for cache in self.data.values():
                # first, a check
                if not hasattr(cache.package, "id") or cache.package.id is None:
                    self.logger.warn(
                        f"Package {cache.package.name} has no ID, skipping"
                    )
                    continue

                pkg_id = cache.package.id
                if pkg_id not in desired_state:
                    desired_state[pkg_id] = set()

                for url in cache.urls:
                    desired_state[pkg_id].add(url)

            return desired_state

        def diff(
            current_state: CurrentURLs, desired_state: Dict[UUID, Set[URL]]
        ) -> DiffResult:
            """
            Returns a DiffResult object with the new URLs, new package-URL links,
            and package-URL links to update.
            """
            new_urls: List[URL] = []
            new_package_urls: Dict[Tuple[UUID, UUID], PackageURL] = {}
            urls_to_update: Dict[Tuple[UUID, UUID], PackageURL] = {}

            for pkg_id, urls in desired_state.items():
                # what are the current URLs for this package?
                current_package_urls = current_state.package_urls.get(pkg_id)

                # let's make the current URLs a dictionary of URL ID to PackageURL
                # object, so that it's easy to figure out which PackageURL we need
                # to update later
                current_urls = {
                    current_package_url.url_id: current_package_url
                    for current_package_url in current_package_urls
                }

                # what are the desired URLs for this package?
                for url in urls:
                    # does this url exist in current?
                    url_obj = current_state.url_map.get((url.url, url.url_type_id))

                    # if not:
                    if not url_obj:
                        # track as a new URL
                        new_url = URL(
                            id=uuid4(), url=url.url, url_type_id=url.url_type_id
                        )
                        new_urls.append(new_url)

                        # we'll use this ID to link the package to the URL
                        url_id = new_url.id
                    else:
                        url_id = url_obj.id

                    # cool, so we have the ID now. we also know if we need to create it.
                    # now, let's do the diff to check if this URL is already linked to
                    # this package
                    if url_id not in current_urls:
                        if (pkg_id, url_id) not in new_package_urls:
                            new_package_url = PackageURL(
                                id=uuid4(),
                                package_id=pkg_id,
                                url_id=url_id,
                                created_at=self.now,
                                updated_at=self.now,
                            )
                            new_package_urls[(pkg_id, url_id)] = new_package_url
                    else:
                        # if it's already linked, just update the updated_at for now
                        # TODO: I think this we should have a latest tag in this table
                        # so we don't need to constantly ensure we're doing this update
                        to_update = current_urls[url_id]
                        to_update.updated_at = self.now
                        if (pkg_id, url_id) not in urls_to_update:
                            urls_to_update[(pkg_id, url_id)] = to_update

            result = DiffResult(
                new_urls=new_urls,
                new_package_urls=list(new_package_urls.values()),
                urls_to_update=list(urls_to_update.values()),
            )
            return result

        # first, get the desired state of all the URL relationships
        desired_state = get_desired_state()

        # check if the URL strings from the above exist in the current state
        desired_urls = set(url.url for urls in desired_state.values() for url in urls)
        current_state = get_current_urls(desired_urls)

        # now, let's do the diff
        result = diff(current_state, desired_state)

        self.logger.debug(f"{len(result.new_urls)} new URLs")
        self.logger.debug(f"{len(result.new_package_urls)} new package-URL links")
        self.logger.debug(f"{len(result.urls_to_update)} package-URL links to update")

        with self.session() as session:
            try:
                # don't anticpate errors bc we're doing the logic now
                if result.new_urls:
                    self.logger.debug("Inserting new URLs")
                    values = [url.to_dict_v2() for url in result.new_urls]
                    stmt = pg_insert(URL).values(values)
                    session.execute(stmt)

                if result.new_package_urls:
                    self.logger.debug("Inserting new package-URL links")
                    values = [
                        pkg_url.to_dict_v2() for pkg_url in result.new_package_urls
                    ]
                    stmt = pg_insert(PackageURL).values(values)
                    session.execute(stmt)

                if result.urls_to_update:
                    self.logger.debug("Updating package-URL links")
                    values = [
                        {"id": pkg_url.id, "updated_at": pkg_url.updated_at}
                        for pkg_url in result.urls_to_update
                    ]
                    # values has the primary key, so per
                    # https://docs.sqlalchemy.org/en/20/orm/queryguide/dml.html#orm-queryguide-bulk-update
                    # I should be able to just use the values directly
                    stmt = update(PackageURL)
                    session.execute(stmt, values)

                session.commit()

            except Exception as e:
                self.logger.error(f"Error inserting URLs or PackageURLs: {str(e)}")

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


if __name__ == "__main__":
    from core.config import PackageManager

    config = Config(PackageManager.PKGX)
    db = DB(config)
    loader = PkgxLoader(config, {})
    loader.load_urls_v2()
