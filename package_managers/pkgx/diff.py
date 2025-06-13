#!/usr/bin/env pkgx uv run

from datetime import datetime
from uuid import UUID, uuid4

from core.config import Config
from core.logger import Logger
from core.models import URL, LegacyDependency, Package, PackageURL
from core.structs import Cache
from package_managers.pkgx.parser import PkgxPackage


class PkgxDiff:
    def __init__(self, config: Config, caches: Cache):
        self.config = config
        self.now = datetime.now()
        self.caches = caches
        self.logger = Logger("pkgx_diff")

    def diff_pkg(self, import_id: str, pkg: PkgxPackage) -> tuple[UUID, Package | None, dict | None]:
        """
        Checks if the given pkg is in the package_cache.

        Returns:
          - pkg_id: the id of the package
          - package: If new, returns a new package object. If existing, returns None
          - changes: a dictionary of changes
        """
        self.logger.debug(f"Diffing package: {import_id}")
        
        if import_id not in self.caches.package_map:
            # new package
            p = Package(
                id=uuid4(),
                derived_id=f"pkgx/{import_id}",
                name=import_id,
                package_manager_id=self.config.pm_config.pm_id,
                import_id=import_id,
                readme=pkg.description,
                created_at=self.now,
                updated_at=self.now,
            )
            pkg_id: UUID = p.id
            return pkg_id, p, {}
        else:
            p = self.caches.package_map[import_id]
            pkg_id = p.id
            # check for changes
            # check if description changed
            if p.readme != pkg.description:
                self.logger.debug(f"Description changed for {import_id}")
                return (
                    pkg_id,
                    None,
                    {"id": p.id, "readme": pkg.description, "updated_at": self.now},
                )
            else:
                # existing package, no change
                return pkg_id, None, None

    def diff_url(
        self, import_id: str, pkg: PkgxPackage, new_urls: dict[tuple[str, UUID], URL]
    ) -> dict[UUID, UUID]:
        """Given a package's URLs, returns the resolved URL for this specific package"""
        resolved_urls: dict[UUID, UUID] = {}

        # Collect all URLs from the package
        urls_to_process = []
        
        # Add homepage URL if it exists
        homepage = self._get_homepage_url(import_id, pkg)
        if homepage:
            urls_to_process.append((homepage, self.config.url_types.homepage))

        # Add source URLs from distributables
        for distributable in pkg.distributable:
            if distributable.url:
                clean_url = self._canonicalize_url(distributable.url)
                if clean_url:
                    urls_to_process.append((clean_url, self.config.url_types.source))
                    
                    # If it's a GitHub URL, also add as repository
                    if self._is_github_url(clean_url):
                        urls_to_process.append((clean_url, self.config.url_types.repository))

        # Process each URL
        for url, url_type in urls_to_process:
            url_key = (url, url_type)
            resolved_url_id: UUID
            
            if url_key in new_urls:
                resolved_url_id = new_urls[url_key].id
            elif url_key in self.caches.url_map:
                resolved_url_id = self.caches.url_map[url_key].id
            else:
                self.logger.debug(f"URL {url} for {url_type} is entirely new")
                new_url = URL(
                    id=uuid4(),
                    url=url,
                    url_type_id=url_type,
                    created_at=self.now,
                    updated_at=self.now,
                )
                resolved_url_id = new_url.id
                new_urls[url_key] = new_url

            resolved_urls[url_type] = resolved_url_id

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
                    pu
                    for pu in self.caches.package_urls[pkg_id]
                    if pu.url_id == url_id
                )
                existing_pu.updated_at = self.now
                updates.append({"id": existing_pu.id, "updated_at": self.now})

        return new_links, updates

    def diff_deps(
        self, import_id: str, pkg: PkgxPackage
    ) -> tuple[list[LegacyDependency], list[LegacyDependency]]:
        """
        Takes in a pkgx package and figures out what dependencies have changed.
        
        Returns:
          - new_deps: a list of new dependencies
          - removed_deps: a list of removed dependencies
        """
        new_deps: list[LegacyDependency] = []
        removed_deps: list[LegacyDependency] = []

        # serialize the actual dependencies into a set of tuples
        actual: set[tuple[UUID, UUID]] = set()

        def process_deps(dep_names: list[str], dep_type: UUID) -> None:
            """Helper to process dependencies of a given type"""
            for dep_name in dep_names:
                if not dep_name:
                    continue

                dependency = self.caches.package_map.get(dep_name)
                if not dependency:
                    self.logger.warn(f"{dep_name}, dep of {import_id} is not in cache")
                    continue

                actual.add((dependency.id, dep_type))

        # Process different types of dependencies
        process_deps(pkg.dependencies, self.config.dependency_types.runtime)
        process_deps(pkg.build.dependencies, self.config.dependency_types.build)
        process_deps(pkg.test.dependencies, self.config.dependency_types.test)

        # get the package ID for what we are working with
        package = self.caches.package_map.get(import_id)
        if not package:
            self.logger.warn(f"New package {import_id}, will grab its deps next time")
            return [], []

        pkg_id: UUID = package.id

        # figure out what's new/removed
        existing: set[tuple[UUID, UUID]] = set()
        legacy_links: set[LegacyDependency] = self.caches.dependencies.get(pkg_id, set())
        existing_legacy_map: dict[tuple[UUID, UUID], LegacyDependency] = {}

        for legacy in legacy_links:
            key = (legacy.dependency_id, legacy.dependency_type_id)
            existing_legacy_map[key] = legacy
            existing.add(key)

        # calculate our diffs
        added_tuples: set[tuple[UUID, UUID]] = actual - existing
        removed_tuples: set[tuple[UUID, UUID]] = existing - actual

        # convert these to LegacyDependency objects
        for dep_id, type_id in added_tuples:
            new_dep = LegacyDependency(
                package_id=pkg_id,
                dependency_id=dep_id,
                dependency_type_id=type_id,
                created_at=self.now,
                updated_at=self.now,
            )
            new_deps.append(new_dep)

        for dep_id, type_id in removed_tuples:
            removed_dep = existing_legacy_map.get((dep_id, type_id))
            if removed_dep:
                removed_deps.append(removed_dep)

        return new_deps, removed_deps

    def _get_homepage_url(self, import_id: str, pkg: PkgxPackage) -> str | None:
        """Get homepage URL for a package using the existing transformer logic"""
        # Import the transformer methods for URL handling
        from package_managers.pkgx.transformer import PkgxTransformer
        
        # Create a temporary transformer instance to use its methods
        temp_transformer = PkgxTransformer(self.config, None)
        
        # Try to get homepage from pkgx API
        homepage = temp_transformer.ask_pkgx(import_id)
        if not homepage:
            homepage = temp_transformer.special_case(import_id)
            
        if homepage:
            return temp_transformer.canonicalize(homepage)
        
        return None

    def _canonicalize_url(self, url: str) -> str:
        """Canonicalize URL using transformer logic"""
        from package_managers.pkgx.transformer import PkgxTransformer
        temp_transformer = PkgxTransformer(self.config, None)
        return temp_transformer.canonicalize(url)

    def _is_github_url(self, url: str) -> bool:
        """Check if URL is a GitHub URL"""
        from package_managers.pkgx.transformer import PkgxTransformer
        temp_transformer = PkgxTransformer(self.config, None)
        return temp_transformer.is_github(url)