#!/usr/bin/env pkgx uv run

from datetime import datetime
from uuid import UUID, uuid4

from core.config import Config
from core.logger import Logger
from core.models import URL, LegacyDependency, Package, PackageURL
from core.structs import Cache, URLKey
from core.utils import is_github_url
from package_managers.debian.db import DebianDB
from package_managers.debian.parser import DebianData
from package_managers.debian.structs import Depends


class DebianDiff:
    def __init__(self, config: Config, caches: Cache, db: DebianDB, logger: Logger):
        self.config = config
        self.now = datetime.now()
        self.caches = caches
        self.db = db
        self.logger = logger

    def diff_pkg(
        self, import_id: str, debian_data: DebianData
    ) -> tuple[UUID, Package | None, dict | None]:
        """
        Checks if the given package is in the package_cache.

        Returns:
          - pkg_id: the id of the package
          - package: If new, returns a new package object. If existing, returns None
          - changes: a dictionary of changes (description updates)
        """
        self.logger.debug(f"Diffing package: {import_id}")

        if import_id not in self.caches.package_map:
            # new package
            name = import_id.split("/")[1]
            p = Package(
                id=uuid4(),
                derived_id=import_id,
                name=name,
                package_manager_id=self.config.pm_config.pm_id,
                import_id=import_id,
                readme=debian_data.description,
                created_at=self.now,
                updated_at=self.now,
            )
            pkg_id: UUID = p.id
            return pkg_id, p, {}
        else:
            # the package exists, check if description has changed
            existing_pkg = self.caches.package_map[import_id]
            pkg_id = existing_pkg.id

            # Check if description (readme) has changed
            if existing_pkg.readme != debian_data.description:
                update_payload = {
                    "id": pkg_id,
                    "readme": debian_data.description,
                    "updated_at": self.now,
                }
                return pkg_id, None, update_payload
            else:
                return pkg_id, None, None

    def diff_url(
        self, import_id: str, debian_data: DebianData, new_urls: dict[URLKey, URL]
    ) -> dict[UUID, UUID]:
        """Given a package's URLs, returns the resolved URL for this specific package"""
        resolved_urls: dict[UUID, UUID] = {}

        # Generate the URLs for this package
        urls = self._generate_chai_urls(debian_data)

        # Process each URL
        for url_key in urls:
            # guard: _generate_chai_urls could be None for a url type
            if url_key is None:
                continue

            resolved_url_id: UUID

            if url_key in new_urls:
                resolved_url_id = new_urls[url_key].id
            elif url_key in self.caches.url_map:
                resolved_url_id = self.caches.url_map[url_key].id
            else:
                self.logger.debug(
                    f"URL {url_key.url} as {url_key.url_type_id} is entirely new"
                )
                new_url = URL(
                    id=uuid4(),
                    url=url_key.url,
                    url_type_id=url_key.url_type_id,
                    created_at=self.now,
                    updated_at=self.now,
                )
                resolved_url_id = new_url.id
                new_urls[url_key] = new_url

            resolved_urls[url_key.url_type_id] = resolved_url_id

        return resolved_urls

    def diff_pkg_url(
        self, pkg_id: UUID, resolved_urls: dict[UUID, UUID]
    ) -> tuple[list[PackageURL], list[dict]]:
        """Takes in a package_id and resolved URLs from diff_url, and generates
        new PackageURL objects as well as a list of changes to existing ones"""

        new_links: list[PackageURL] = []
        updates: list[dict] = []

        # what are the existing links?
        existing: set[UUID] = {
            pu.url_id for pu in self.caches.package_urls.get(pkg_id, set())
        }

        # for each URL type/URL for this package:
        for _url_type, url_id in resolved_urls.items():
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
                # existing link - update timestamp
                existing_pu = next(
                    pu for pu in self.caches.package_urls[pkg_id] if pu.url_id == url_id
                )
                existing_pu.updated_at = self.now
                updates.append({"id": existing_pu.id, "updated_at": self.now})

        return new_links, updates

    def diff_deps(
        self, import_id: str, debian_data: DebianData
    ) -> tuple[list[LegacyDependency], list[LegacyDependency]]:
        """
        Takes in a debian package and figures out what dependencies have changed.

        The process is:
           1. Build a view of what the package's dependencies are according to
              the parsed debian data, using priority-based deduplication
           2. Get this package's ID from CHAI
           3. Get this package's existing dependencies from CHAI
           4. Compare the two sets, and identify new and removed dependencies

        Note: The database has a unique constraint on (package_id, dependency_id),
        so if a package depends on the same dependency with multiple types (e.g.,
        both runtime and build), we choose the highest priority type:
        Runtime > Build > Test

        Returns:
          - new_deps: a list of new dependencies
          - removed_deps: a list of removed dependencies
        """
        # First, collect all dependencies and deduplicate by dependency name
        # choosing the highest priority dependency type for each unique dependency
        dependency_map: dict[str, UUID] = {}

        # Priority order: Runtime > Build > Test
        priority_order = {
            self.config.dependency_types.runtime: 1,
            self.config.dependency_types.build: 2,
            self.config.dependency_types.test: 3,
        }

        def process_deps(dependencies: list[Depends], dep_type: UUID) -> None:
            """Helper to process dependencies of a given type with priority"""
            for dep in dependencies:
                dep_name = f"debian/{dep.package}"  # bc the map is by import_id

                # Get the dependency package from cache
                dependency = self.caches.package_map.get(dep_name)

                # try debian/dependency
                if not dependency:
                    self.logger.debug(f"{dep_name} not loaded, will catch next time")
                    continue

                # If this dependency already exists in our map, choose higher priority
                if dep_name in dependency_map:
                    existing_priority = priority_order.get(
                        dependency_map[dep_name], 999
                    )
                    new_priority = priority_order.get(dep_type, 999)

                    if new_priority < existing_priority:  # Lower is better!
                        old_type_id = dependency_map[dep_name]
                        dependency_map[dep_name] = dep_type
                        self.logger.debug(
                            f"Updated dependency type for {dep_name} from "
                            f"{old_type_id} to {dep_type} (higher priority)"
                        )
                else:
                    dependency_map[dep_name] = dep_type

        # Process different types of dependencies with priority handling
        # Debian has: depends (runtime), build_depends (build), recommends, suggests, etc.
        process_deps(debian_data.depends, self.config.dependency_types.runtime)
        process_deps(debian_data.build_depends, self.config.dependency_types.build)
        # Map recommends and suggests to runtime for simplicity
        process_deps(debian_data.recommends, self.config.dependency_types.runtime)
        process_deps(debian_data.suggests, self.config.dependency_types.runtime)

        # Now build the actual set of dependencies with resolved types
        actual: set[tuple[UUID, UUID]] = set()
        for dep_name, dep_type in dependency_map.items():
            dependency = self.caches.package_map.get(dep_name)
            if dependency:  # Double-check it still exists
                actual.add((dependency.id, dep_type))

        # get the package ID for what we are working with
        package = self.caches.package_map.get(import_id)
        if not package:
            self.logger.debug(f"New package {import_id}, will grab its deps next time")
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
            except StopIteration as exc:
                cache_deps_str = "\n".join(
                    [
                        f"{dep.dependency_id} / {dep.dependency_type_id}"
                        for dep in cache_deps
                    ]
                )
                raise ValueError(
                    f"Removing {removed_dep_id} / {removed_dep_type} for {pkg_id} but not in Cache: \n{cache_deps_str}"
                ) from exc

        return new_deps, removed_deps

    def _generate_chai_urls(self, debian_data: DebianData) -> list[URLKey]:
        """Generate URLs for a debian package"""
        urls = []

        # Homepage URL
        if debian_data.homepage:
            urls.append(URLKey(debian_data.homepage, self.config.url_types.homepage))

        # Source URL
        source_url = (
            debian_data.vcs_git if debian_data.vcs_git else debian_data.vcs_browser
        )
        if source_url:
            urls.append(URLKey(source_url, self.config.url_types.source))

        # Repository URL
        if is_github_url(source_url):
            urls.append(URLKey(source_url, self.config.url_types.repository))

        return urls
