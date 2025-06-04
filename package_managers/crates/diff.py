from datetime import datetime
from uuid import UUID, uuid4

from core.config import Config
from core.logger import Logger
from core.models import URL, LegacyDependency, Package, PackageURL
from core.structs import Cache, URLKey
from package_managers.crates.structs import Crate, DependencyType


class Diff:
    def __init__(self, config: Config, caches: Cache):
        self.config = config
        self.now = datetime.now()
        self.caches = caches
        self.logger = Logger("crates_diff")

    def diff_pkg(self, pkg: Crate) -> tuple[UUID, Package | None, dict | None]:
        """
        Checks if the given pkg is in the package_cache.

        Returns:
            pkg_id: UUID, the id of the package in the db
            pkg_obj: Package | None, the package object if it's new
            update_payload: dict | None, the update payload if it's an update
        """
        pkg_id: UUID
        crate_id: str = str(pkg.id)  # import_ids are strings in the db
        if crate_id not in self.caches.package_map:
            # new package
            p = Package(
                id=uuid4(),
                derived_id=f"crates/{pkg.name}",
                name=pkg.name,
                package_manager_id=self.config.pm_config.pm_id,
                import_id=crate_id,
                readme=pkg.readme,
                created_at=self.now,
                updated_at=self.now,
            )
            pkg_id = p.id
            return pkg_id, p, {}
        else:
            # it's in the cache, so check for changes
            p = self.caches.package_map[crate_id]
            pkg_id = p.id
            # check for changes
            # right now, that's just the readme
            if p.readme != pkg.readme:
                return (
                    pkg_id,
                    None,
                    {"id": p.id, "readme": pkg.readme, "updated_at": self.now},
                )
            else:
                # existing package, no change
                return pkg_id, None, None

    def diff_url(self, pkg: Crate, new_urls: dict[URLKey, URL]) -> dict[UUID, UUID]:
        """
        Identifies the correct URL for this crate, based on fetched data and all URL
        strings collected so far

        Returns:
            resolved_urls: dict[UUID, UUID], the resolved URL for this crate
        """
        resolved_urls: dict[UUID, UUID] = {}

        urls: list[URLKey] = [
            URLKey(pkg.homepage, self.config.url_types.homepage),
            URLKey(pkg.repository, self.config.url_types.repository),
            URLKey(pkg.documentation, self.config.url_types.documentation),
            URLKey(pkg.source, self.config.url_types.source),
        ]

        for url_key in urls:
            url = url_key.url
            url_type = url_key.url_type_id

            # guard: no URL
            if not url:
                continue

            resolved_url_id: UUID

            if url_key in new_urls:
                # if we've already tried to create this URL, use that one
                resolved_url_id = new_urls[url_key].id
            elif url_key in self.caches.url_map:
                # if it's already in the database, let's use that one
                resolved_url_id = self.caches.url_map[url_key].id
            else:
                # most will be here because it's the first run of clean data
                # BIG HONKING TODO: uncomment this later
                # self.logger.debug(f"URL {url} for {url_type} is entirely new")
                # end of BIG HONKING TODO
                new_url = URL(
                    id=uuid4(),
                    url=url,
                    url_type_id=url_type,
                    created_at=self.now,
                    updated_at=self.now,
                )
                resolved_url_id = new_url.id

                # NOTE: THIS IS SUPER IMPORTANT
                # we're adding to new_urls here, not just in main
                new_urls[url_key] = new_url

            resolved_urls[url_type] = resolved_url_id

        return resolved_urls

    def diff_pkg_url(
        self, pkg_id: UUID, resolved_urls: dict[UUID, UUID]
    ) -> tuple[list[PackageURL], list[dict]]:
        """Takes in a package_id and resolved URLs from diff_url, and generates
        new PackageURL objects as well as a list of changes to existing ones

        Inputs:
          - pkg_id: the id of the package
          - resolved_urls: a map of url types to final URL ID for this pkg

        Outputs:
          - new_package_urls: a list of new PackageURL objects
          - updated_package_urls: a list of changes to existing PackageURL objects

        TODO:
          - We're updating every single package_url entity, which takes time. We should
            check if the latest URL has changed, and if so, only update that one.
        """
        new_links: list[PackageURL] = []
        updates: list[dict] = []

        # what are the existing links?
        existing: set[UUID] = {
            pu.url_id for pu in self.caches.package_urls.get(pkg_id, set())
        }

        # for the correct URL type / URL for this package:
        for url_type, url_id in resolved_urls.items():
            if url_id not in existing:
                # new link!
                new_links.append(
                    PackageURL(
                        id=uuid4(),
                        package_id=pkg_id,
                        url_id=url_id,
                        created_at=self.now,
                        updated_at=self.now,
                    )
                )
            else:
                # TODO: this should only happen for `latest` URLs
                # there is an existing link between this URL and this package
                # let's find it
                existing_pu = next(
                    pu for pu in self.caches.package_urls[pkg_id] if pu.url_id == url_id
                )
                existing_pu.updated_at = self.now
                updates.append({"id": existing_pu.id, "updated_at": self.now})

        return new_links, updates

    def diff_deps(
        self, pkg: Crate
    ) -> tuple[list[LegacyDependency], list[LegacyDependency]]:
        """
        Identifies new and removed dependencies for a given crate

        The process is:
           1. Build a view of what the package's dependencies are according to
              the crates.io database.
           2. Get this crate's Package ID from CHAI
           3. Get this crate's existing dependencies from CHAI
           4. Compare the two sets, and identify new and removed dependencies

        Note: The database has a unique constraint on (package_id, dependency_id),
        so if a package depends on the same dependency with multiple types (e.g.,
        both runtime and build), we choose the highest priority type:
        NORMAL (runtime) > BUILD > DEV

        Returns:
            new_deps: list[LegacyDependency], the new dependencies
            removed_deps: list[LegacyDependency], the removed dependencies
        """
        new_deps: list[LegacyDependency] = []
        removed_deps: list[LegacyDependency] = []

        # First, collect all dependencies and deduplicate by (package_id, dependency_id)
        # choosing the highest priority dependency type for each unique dependency
        dependency_map: dict[UUID, DependencyType] = {}

        # Priority order: NORMAL (runtime) > BUILD > DEV
        priority_order = {
            DependencyType.NORMAL: 1,
            DependencyType.BUILD: 2,
            DependencyType.DEV: 3,
        }

        # Build the map of dependencies, keeping only the highest priority type
        for dependency in pkg.latest_version.dependencies:
            dep_crate_id: str = str(dependency.dependency_id)
            dep_type: DependencyType = dependency.dependency_type

            # guard: no dep_id
            if not dep_crate_id:
                raise ValueError(f"No dep_id for {dependency}")

            # guard: no dep_type
            if dep_type is None:
                raise ValueError(f"No dep_type for {dependency}")

            # get the ID from the cache
            dependency_pkg = self.caches.package_map.get(dep_crate_id)

            # if we don't have the dependency, skip it for now
            if not dependency_pkg:
                self.logger.debug(f"{dep_crate_id}, dependency of {pkg.name} is new")
                continue

            dependency_id = dependency_pkg.id

            # If this dependency already exists in our map, choose the higher priority type
            if dependency_id in dependency_map:
                existing_priority = priority_order.get(
                    dependency_map[dependency_id], 999
                )
                new_priority = priority_order.get(dep_type, 999)

                if new_priority < existing_priority:  # Lower number = higher priority
                    old_type = dependency_map[dependency_id]
                    dependency_map[dependency_id] = dep_type
                    self.logger.debug(
                        f"Updated dependency type for {dep_crate_id} from "
                        f"{old_type} to {dep_type} (higher priority)"
                    )
            else:
                dependency_map[dependency_id] = dep_type

        # Now build the actual set of dependencies with resolved types
        actual: set[tuple[UUID, UUID]] = set()
        for dependency_id, dep_type in dependency_map.items():
            # figure out the dependency type UUID
            dependency_type = self._resolve_dep_type(dep_type)
            # add it to the set of actual dependencies
            actual.add((dependency_id, dependency_type))

        # establish the package that we are working with
        crate_id: str = str(pkg.id)
        package = self.caches.package_map.get(crate_id)
        if not package:
            # TODO: handle this case, though it fixes itself on the next run
            self.logger.debug(f"New package {pkg.name}, will grab its deps next time")
            return [], []

        pkg_id: UUID = package.id

        # what are its existing dependencies?
        # specifically, existing dependencies IN THE SAME STRUCTURE as `actual`,
        # so we can do an easy comparison
        existing: set[tuple[UUID, UUID]] = {
            (dep.dependency_id, dep.dependency_type_id)
            for dep in self.caches.dependencies.get(pkg_id, set())
        }

        # we have two sets!
        # actual minus existing = new_deps
        # existing minus actual = removed_deps
        new = actual - existing
        removed = existing - actual

        new_deps: list[LegacyDependency] = [
            LegacyDependency(
                # don't include the ID because it's a sequence for this table
                package_id=pkg_id,
                dependency_id=dep[0],
                dependency_type_id=dep[1],
                created_at=self.now,
                updated_at=self.now,
            )
            for dep in new
        ]

        # get the existing legacy dependency, and add it to removed_deps
        removed_deps: list[LegacyDependency] = []
        cache_deps: set[LegacyDependency] = self.caches.dependencies.get(pkg_id, set())
        for removed_dep_id, removed_dep_type in removed:
            try:
                existing_dep = next(
                    dep
                    for dep in cache_deps
                    if dep.dependency_id == removed_dep_id
                    and dep.dependency_type_id == removed_dep_type
                )

                removed_deps.append(existing_dep)
            except StopIteration:
                cache_deps_str = "\n".join(
                    [
                        f"{dep.dependency_id} / {dep.dependency_type_id}"
                        for dep in cache_deps
                    ]
                )
                raise ValueError(
                    f"Removing {removed_dep_id} / {removed_dep_type} for {pkg_id} but not in Cache: \n{cache_deps_str}"  # noqa: E501
                )

        return new_deps, removed_deps

    def _resolve_dep_type(self, dep_type: DependencyType) -> UUID:
        """
        Resolves the dependency type UUID from the config
        """
        if dep_type == DependencyType.NORMAL:
            return self.config.dependency_types.runtime
        elif dep_type == DependencyType.BUILD:
            return self.config.dependency_types.build
        elif dep_type == DependencyType.DEV:
            return self.config.dependency_types.development
        else:
            raise ValueError(f"Unknown dependency type: {dep_type}")
