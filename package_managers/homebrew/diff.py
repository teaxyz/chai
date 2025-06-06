from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple
from uuid import UUID, uuid4

from core.config import Config
from core.logger import Logger
from core.models import URL, LegacyDependency, Package, PackageURL
from core.structs import Cache
from package_managers.homebrew.structs import Actual


class Diff:
    def __init__(self, config: Config, caches: Cache):
        self.config = config
        self.now = datetime.now()
        self.caches = caches
        self.logger = Logger("homebrew_diff")

    def diff_pkg(self, pkg: Actual) -> Tuple[UUID, Optional[Package], Optional[Dict]]:
        """
        Checks if the given pkg is in the package_cache.

        Returns:
          - pkg_id: the id of the package
          - package: If new, returns a new package object. If existing, returns None
          - changes: a dictionary of changes
        """
        self.logger.debug(f"Diffing package: {pkg.formula}")
        pkg_id: UUID
        if pkg.formula not in self.caches.package_map:
            # new package
            p = Package(
                id=uuid4(),
                derived_id=f"homebrew/{pkg.formula}",
                name=pkg.formula,
                package_manager_id=self.config.pm_config.pm_id,
                import_id=pkg.formula,
                readme=pkg.description,
                created_at=self.now,
                updated_at=self.now,
            )
            pkg_id: UUID = p.id
            # no update payload, so that's empty
            return pkg_id, p, {}
        else:
            p = self.caches.package_map[pkg.formula]
            pkg_id = p.id
            # check for changes
            # right now, that's just the readme
            if p.readme != pkg.description:
                self.logger.debug(f"Description changed for {pkg.formula}")
                return (
                    pkg_id,
                    None,
                    {"id": p.id, "readme": pkg.description, "updated_at": self.now},
                )
            else:
                # existing package, no change
                return pkg_id, None, None

    def diff_url(
        self, pkg: Actual, new_urls: Dict[Tuple[str, UUID], URL]
    ) -> Dict[UUID, UUID]:
        """Given a package's URLs, returns the resolved URL or this specific formula"""
        resolved_urls: Dict[UUID, UUID] = {}

        # we need to check if (a) URLs are in our cache, or (b) if we've already handled
        # them before. if so, we should use that
        urls = (
            (pkg.homepage, self.config.url_types.homepage),
            (pkg.source, self.config.url_types.source),
            (pkg.repository, self.config.url_types.repository),
        )

        for url, url_type in urls:
            # guard: no URL
            if not url:
                continue

            url_key = (url, url_type)
            resolved_url_id: UUID
            if url_key in new_urls:
                resolved_url_id = new_urls[url_key].id
            elif url_key in self.caches.url_cache:
                resolved_url_id = self.caches.url_cache[url_key].id
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

                # NOTE: THIS IS SUPER IMPORTANT
                # we're not just borrowing this value, we're mutating it as well
                new_urls[url_key] = new_url

            resolved_urls[url_type] = resolved_url_id

        return resolved_urls

    def diff_pkg_url(
        self, pkg_id: UUID, resolved_urls: Dict[UUID, UUID]
    ) -> Tuple[List[PackageURL], List[Dict]]:
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
        new_links: List[PackageURL] = []
        updates: List[Dict] = []

        # what are the existing links?
        existing: Set[UUID] = {
            pu.url_id for pu in self.caches.package_url_cache.get(pkg_id, set())
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
                # here is an existing link between this URL and this package
                # let's find it
                existing_pu = next(
                    pu
                    for pu in self.caches.package_url_cache[pkg_id]
                    if pu.url_id == url_id
                )
                existing_pu.updated_at = self.now
                updates.append({"id": existing_pu.id, "updated_at": self.now})

        return new_links, updates

    def diff_deps(
        self, pkg: Actual
    ) -> Tuple[List[LegacyDependency], List[LegacyDependency]]:
        """
        Takes in a Homebrew formula and figures out what dependencies have changed. Also
        uses the LegacyDependency table, because that is package to package.

        Warnings:
          - Updates show up as removed + new
          - This is Homebrew specific, since LegacyDependency mandates uniqueness
            from package_id -> dependency_id, but Homebrew allows duplicate
            dependencies across multiple dependency types. So we've got a process helper
            that handles this.

        Returns:
          - new_deps: a list of new dependencies
          - removed_deps: a list of removed dependencies
        """
        new_deps: List[LegacyDependency] = []
        removed_deps: List[LegacyDependency] = []

        # serialize the actual dependencies into a set of tuples
        actual: Set[Tuple[UUID, UUID]] = set()
        processed: Set[str] = set()

        def process(dep_names: Optional[List[str]], dep_type: UUID) -> None:
            """Helper to process dependencies of a given type"""
            # guard: no dependencies
            if not dep_names:
                return

            for name in dep_names:
                # guard: no dependency name / empty name
                if not name:
                    continue

                # means one dependency is build and test, for example
                # see https://formulae.brew.sh/api/formula/abook.json for example
                # gettext is both a build and runtime dependency
                if name in processed:
                    continue

                dependency = self.caches.package_map.get(name)

                # guard: no dependency
                if not dependency:
                    # TODO: handle this case, though it fixes itself on the next run
                    self.logger.warn(f"{name}, dep of {pkg.formula} is new")
                    continue

                actual.add((dependency.id, dep_type))
                processed.add(name)

        # alright, let's do it
        if hasattr(pkg, "dependencies"):
            process(pkg.dependencies, self.config.dependency_types.runtime)
        if hasattr(pkg, "build_dependencies"):
            process(pkg.build_dependencies, self.config.dependency_types.build)
        if hasattr(pkg, "test_dependencies"):
            process(pkg.test_dependencies, self.config.dependency_types.test)
        if hasattr(pkg, "recommended_dependencies"):
            process(
                pkg.recommended_dependencies, self.config.dependency_types.recommended
            )
        if hasattr(pkg, "optional_dependencies"):
            process(pkg.optional_dependencies, self.config.dependency_types.optional)

        # get the package ID for what we are working with
        package = self.caches.package_map.get(pkg.formula)
        if not package:
            # TODO: handle this case, though it fixes itself on the next run
            self.logger.warn(f"New package {pkg.formula}, will grab its deps next time")
            return [], []

        pkg_id: UUID = package.id

        # now, we need to figure out what's new / removed
        # we need:
        # 1. something in that same structure as `actual`, to track what's in CHAI
        existing: Set[Tuple[UUID, UUID]] = set()
        # 2. set of LegacyDependency objects
        legacy_links: Set[LegacyDependency] = self.caches.dependencies.get(
            pkg_id, set()
        )
        # 3. easy look up to get to legacy_links to go from 1 to 2
        existing_legacy_map: Dict[Tuple[UUID, UUID], LegacyDependency] = {}

        for legacy in legacy_links:
            key = (legacy.dependency_id, legacy.dependency_type_id)
            existing_legacy_map[key] = legacy
            existing.add(key)

        # calculate our diffs
        added_tuples: Set[Tuple[UUID, UUID]] = actual - existing
        removed_tuples: Set[Tuple[UUID, UUID]] = existing - actual

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
