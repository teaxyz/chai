#! /usr/bin/env pkgx +python@3.11 uv run

import re
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple, Union
from uuid import UUID, uuid4

from permalint import normalize_url
from requests import get
from sqlalchemy import select, update

from core.config import Config, PackageManager
from core.db import DB, CurrentURLs
from core.logger import Logger
from core.models import URL, LegacyDependency, Package, PackageURL


@dataclass
class Actual:
    formula: str
    description: str
    license: str
    homepage: str
    source: str
    repository: Optional[str]
    build_dependencies: Optional[List[str]]
    dependencies: Optional[List[str]]
    test_dependencies: Optional[List[str]]
    recommended_dependencies: Optional[List[str]]
    optional_dependencies: Optional[List[str]]
    uses_from_macos: Optional[List[str]]
    conflicts_with: Optional[List[str]]


# TODO: we **could** move these to the core folder, but we're trying this out with
# Homebrew first
@dataclass
class CurrentGraph:
    package_map: Dict[str, Package]
    dependencies: Dict[UUID, Set[LegacyDependency]]


@dataclass
class Cache:
    package_cache: Dict[str, Package]
    url_cache: Dict[Tuple[str, UUID], UUID]
    package_url_cache: Dict[UUID, Set[PackageURL]]
    dependency_cache: Dict[UUID, Set[LegacyDependency]]


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
        pkg_id: UUID
        if pkg.formula not in self.caches.package_cache:
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
            p = self.caches.package_cache[pkg.formula]
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

        # we need to check if these URLs are in our cache, or if we've already handled
        # them before
        urls = (
            (pkg.homepage, self.config.url_types.homepage),
            (pkg.source, self.config.url_types.source),
            (pkg.repository, self.config.url_types.repository),
        )

        for url, url_type in urls:
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


class HomebrewDB(DB):
    def __init__(self, logger_name: str, config: Config):
        super().__init__(logger_name)
        self.config = config
        self.cache: CurrentGraph = self.current_graph()
        self.logger.log(f"{len(self.cache.package_map)} packages from Homebrew")

    def current_graph(self) -> CurrentGraph:
        """Get the Homebrew packages and dependencies"""
        package_map: Dict[str, Package] = {}  # name to package
        dependencies: Dict[UUID, Set[LegacyDependency]] = {}

        stmt = (
            select(Package, LegacyDependency)
            .select_from(Package)
            .join(
                LegacyDependency,
                onclause=Package.id == LegacyDependency.package_id,
                isouter=True,
            )
            .where(Package.package_manager_id == self.config.pm_config.pm_id)
        )

        with self.session() as session:
            result = session.execute(stmt)

            for pkg, dep in result:
                # add to the package map
                if pkg.name not in package_map:
                    package_map[pkg.name] = pkg

                # and add to the dependencies map as well
                if dep:  # check because it's an outer join
                    if pkg.id not in dependencies:
                        dependencies[pkg.id] = set()
                    dependencies[pkg.id].add(dep)

        return CurrentGraph(package_map=package_map, dependencies=dependencies)

    def set_current_urls(self, urls: CurrentURLs) -> None:
        """Wrapper for setting current urls"""
        self.current_urls: CurrentURLs = self.get_current_urls(urls)
        self.logger.debug(f"Found {len(self.current_urls.url_map)} Homebrew URLs")

    def ingest(
        self,
        new_packages: List[Package],
        new_urls: List[URL],
        new_package_urls: List[PackageURL],
        updated_packages: List[Dict[str, Union[UUID, str, datetime]]],
        updated_package_urls: List[Dict[str, Union[UUID, datetime]]],
    ) -> None:
        """
        Ingest the diffs by first adding all new entities, then updating existing ones.

        Inputs:
          - diffs: a list of Diff objects

        Outputs:
          - None
        """
        self.logger.log("-" * 100)
        self.logger.log("Going to load")
        self.logger.log(f"New packages: {len(new_packages)}")
        self.logger.log(f"New URLs: {len(new_urls)}")
        self.logger.log(f"New package URLs: {len(new_package_urls)}")
        self.logger.log(f"Updated packages: {len(updated_packages)}")
        self.logger.log(f"Updated package URLs: {len(updated_package_urls)}")
        self.logger.log("-" * 100)

        with self.session() as session:
            try:
                # 1. Add all new objects with granular flushes
                if new_packages:
                    session.add_all(new_packages)
                    session.flush()

                if new_urls:
                    session.add_all(new_urls)
                    session.flush()

                if new_package_urls:
                    session.add_all(new_package_urls)
                    session.flush()

                # 2. Perform updates (these will now operate on a flushed state)
                if updated_packages:
                    session.execute(update(Package), updated_packages)

                try:
                    if updated_package_urls:
                        session.execute(update(PackageURL), updated_package_urls)
                except Exception as e:
                    self.logger.error(
                        f"Error during updated package URL ingestion: {e}"
                    )
                    session.rollback()

                # 3. Commit all changes
                session.commit()
                self.logger.log("✅ Successfully ingested")
            except Exception as e:
                self.logger.error(f"Error during batched ingest: {e}")
                session.rollback()
                # raise # Commented out to allow processing to continue after an error


def homebrew(config: Config) -> List[Actual]:
    """Get the current state of Homebrew"""
    response = get(config.pm_config.source)
    try:
        response.raise_for_status()
    except Exception as e:
        print(e)
        raise e

    # make json
    data = response.json()

    # prep results
    results: List[Actual] = []

    for formula in data:
        # check if deprecated
        # TODO; should we do anything about these?
        deprecated = formula.get("deprecated", False)
        if deprecated:
            continue

        # create temp vars for stuff we transform...basically URL
        homepage = normalize_url(formula["homepage"])

        # try urls.head.url, because that generally points to GitHub / git
        # use urls.stable.url as a backstop
        source = normalize_url(
            formula["urls"].get("head", formula["urls"]["stable"]).get("url", "")
        )

        # collect github / gitlab repos
        if re.search(r"^github.com", source) or re.search(r"^gitlab.com", source):
            repository = source
        else:
            repository = None

        # create the actual
        actual = Actual(
            formula=formula["name"],
            description=formula["desc"],
            license=formula["license"],
            homepage=homepage,
            source=source,
            repository=repository,
            build_dependencies=formula["build_dependencies"],
            dependencies=formula["dependencies"],
            test_dependencies=formula["test_dependencies"],
            recommended_dependencies=formula["recommended_dependencies"],
            optional_dependencies=formula["optional_dependencies"],
            uses_from_macos=formula["uses_from_macos"],
            conflicts_with=formula["conflicts_with"],
        )

        results.append(actual)

    return results


def main(config: Config, db: HomebrewDB) -> None:
    """A new attempt at loading Homebrew

    - first get the current state: packages, urls, dependencies
    - then use homebrew's formula.json to get the new state
    - grab a diff for each object
    - send everything to the loader
    """
    logger = Logger("homebrew_main")
    brew = homebrew(config)

    # get the URLs & set that
    brew_urls = set(b.source for b in brew) | set(b.homepage for b in brew)
    db.set_current_urls(brew_urls)
    logger.log("Set current URLs")

    # get the caches here
    package_cache: Dict[str, Package] = db.cache.package_map
    url_cache: Dict[Tuple[str, UUID], UUID] = db.current_urls.url_map
    package_url_cache: Dict[UUID, Set[PackageURL]] = db.current_urls.package_urls
    # TODO: dependency cache
    cache = Cache(package_cache, url_cache, package_url_cache, {})

    # total set of updates we'll make are:
    new_packages: List[Package] = []
    new_urls: Dict[Tuple[str, UUID], URL] = {}  # we'll convert this later
    new_package_urls: List[PackageURL] = []
    updated_packages: List[Dict[str, Union[UUID, str, datetime]]] = []
    updated_package_urls: List[Dict[str, Union[UUID, datetime]]] = []

    diff = Diff(config, cache)
    for i, pkg in enumerate(brew):
        pkg_id, pkg_obj, update_payload = diff.diff_pkg(pkg)
        if pkg_obj:
            logger.debug(f"New package: {pkg_obj.name}")
            new_packages.append(pkg_obj)
        if update_payload:
            logger.debug(f"Updated package: {update_payload['id']}")
            updated_packages.append(update_payload)

        # NOTE: resolved urls is a map of url types to final URL ID for this pkg
        # also, new_urls gets passed in AND mutated
        # if only there was a programming language that had away to specify that info
        resolved_urls = diff.diff_url(pkg, new_urls)

        # now, new package urls
        new_links, updated_links = diff.diff_pkg_url(pkg_id, resolved_urls)
        if new_links:
            logger.debug(f"New package URLs: {len(new_links)}")
            new_package_urls.extend(new_links)
        if updated_links:
            logger.debug(f"Updated package URLs: {len(updated_links)}")
            updated_package_urls.extend(updated_links)

        if config.exec_config.test and i > 10:
            break

    # cool, all done.
    # final cleanup is to replace the new_urls map with a list
    final_new_urls = list(new_urls.values())

    # send to loader
    db.ingest(
        new_packages,
        final_new_urls,
        new_package_urls,
        updated_packages,
        updated_package_urls,
    )


if __name__ == "__main__":
    config = Config(PackageManager.HOMEBREW)
    db = HomebrewDB("homebrew_db_main", config)
    main(config, db)
