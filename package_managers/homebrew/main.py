#! /usr/bin/env pkgx +python@3.11 uv run

import re
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple, Union
from uuid import UUID, uuid4

from deprecated import deprecated
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


@deprecated(reason="Monolithic for per package diffs, use individual diffs instead")
@dataclass
class DiffLegacy:
    new_package: Optional[Package]
    new_urls: Optional[Set[URL]]
    new_dependencies: Optional[Set[LegacyDependency]]
    new_package_urls: Optional[Set[PackageURL]]
    removed_dependencies: Optional[Set[LegacyDependency]]
    updated_package: Optional[Package]
    updated_package_urls: Optional[Set[PackageURL]]


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
            pkg_id = p.id
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
        self, pkg_id: UUID, url: str, url_type: UUID
    ) -> Tuple[UUID, Optional[PackageURL]]:
        """Placeholder for diffing a single package URL"""
        pass


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

    @deprecated(reason="Monolithic for per package diffs, use individual diffs instead")
    def diff(self, pkg: Actual) -> DiffLegacy:
        """
        Constructs a diff object for a given package, so we can see what's change
        and accordingly proceed with the ingestion.

        Inputs:
          - pkg: the formula from Homebrew's API

        Outputs:
          - diff_result: a Diff object
        """
        self.logger.debug(f"Diffing {pkg.formula}")

        # initialize the diff result
        diff_result = Diff(
            new_package=None,
            new_urls=set(),
            new_dependencies=set(),
            new_package_urls=set(),
            removed_dependencies=set(),
            updated_package=None,
            updated_package_urls=set(),
        )

        # FIRST, is this a new package or existing?
        current = self.cache.package_map.get(pkg.formula)
        pkg_id: UUID
        if current:
            # ok, it exists
            self.logger.debug(f"Package {pkg.formula} already exists")
            pkg_id = current.id

            # check if any changes on the current obj
            # for example, let's keep this
            if current.readme != pkg.description:
                current.readme = pkg.description
                diff_result.updated_package = current
                diff_result.updated_package.updated_at = self.now
                self.logger.debug(f"Description changed for {pkg.formula}")
        else:
            self.logger.debug(f"Package {pkg.formula} is new")
            new = Package(
                id=uuid4(),
                derived_id=f"homebrew/{pkg.formula}",
                name=pkg.formula,
                package_manager_id=self.config.pm_config.pm_id,
                import_id=pkg.formula,
                readme=pkg.description,
                created_at=self.now,
                updated_at=self.now,
            )
            diff_result.new_package = new
            pkg_id = new.id

        # SECOND, let's work on the URLs
        urls: Tuple[str, UUID] = (
            (pkg.homepage, self.config.url_types.homepage),
            (pkg.source, self.config.url_types.source),
            (pkg.repository, self.config.url_types.repository),
        )
        # let's see if they either exist in CHAI and / or are linked to the package

        # grab the current package URL records, and corresponding url_id for this pkg
        package_urls: Set[PackageURL] = self.current_urls.package_urls.get(
            pkg_id, set()
        )
        linked_urls: Set[UUID] = set(pu.url_id for pu in package_urls)

        for url, url_type in urls:
            if not url:  # skip None
                continue

            # check if the URL exists in CHAI
            url_map_key = (url, url_type)
            existing_url = self.current_urls.url_map.get(url_map_key)
            url_id: UUID

            if existing_url:
                url_id = existing_url.id
                self.logger.debug(f"URL {url} for {url_type} already exists")
                # we're ending here because we don't need to modify anything for URLs
            else:
                # so the url / url_type combo is new
                # don't need to check if this combo was added to new_urls because the
                # URL Types we're iterating through are always different
                self.logger.debug(f"URL {url} for {url_type} is entirely new")
                new_url = URL(
                    id=uuid4(),
                    url=url,
                    url_type_id=url_type,
                    created_at=self.now,
                    updated_at=self.now,
                )
                diff_result.new_urls.add(new_url)
                url_id = new_url.id  # and here's our ID!

            # THIRD check if the pkg_id is linked to url_id in Package URLs
            if url_id not in linked_urls:
                new_pkg_url = PackageURL(
                    id=uuid4(),
                    package_id=pkg_id,
                    url_id=url_id,
                    created_at=self.now,
                    updated_at=self.now,
                )
                diff_result.new_package_urls.add(new_pkg_url)
                self.logger.debug(f"New package URL {url} for {url_type}")
            else:
                # the link exists. let's say that we updated it now
                # TODO: we should only do this for `latest` URLs
                existing_pkg_url = next(
                    pu for pu in package_urls if pu.url_id == url_id
                )
                existing_pkg_url.updated_at = self.now
                diff_result.updated_package_urls.add(existing_pkg_url)
                self.logger.debug(f"Updated package URL {url} for {url_type}")

        return diff_result

        # TODO; dependencies
        # any new dependencies?
        # this would depend on whether it's new (all would be new)
        # or not, only ones that changed would be new / remove
        # also, we need to look at all the dependency types
        # also, if it's a new package depending on a new package, it might
        # be a situation, since the new dependency package won't be in the cache
        # but, we can always have a thing that either gets a package id or makes one

    def ingest(self, diffs: List[Diff]) -> None:
        """
        Ingest the diffs by first adding all new entities, then updating existing ones.

        Inputs:
          - diffs: a list of Diff objects

        Outputs:
          - None
        """
        # init the lists
        new_packages: List[Package] = []
        new_urls: List[URL] = []
        new_package_urls: List[PackageURL] = []

        # for updates, store as (id, readme)
        updated_packages: List[Dict[str, Union[UUID, str, datetime]]] = []
        updated_package_urls: List[Dict[str, Union[UUID, datetime]]] = []

        for diff in diffs:
            if diff.new_package:
                new_packages.append(diff.new_package)

            if diff.new_urls:
                new_urls.extend(list(diff.new_urls))

            if diff.new_package_urls:
                new_package_urls.extend(list(diff.new_package_urls))

            if diff.updated_package:
                updated_packages.append(
                    {
                        "id": diff.updated_package.id,
                        "readme": diff.updated_package.readme,
                        "updated_at": diff.updated_package.updated_at,
                    }
                )

            if diff.updated_package_urls:
                updated_package_urls.extend(
                    [
                        {"id": pu.id, "updated_at": self.now}
                        for pu in diff.updated_package_urls
                    ]
                )

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

                if updated_package_urls:
                    session.execute(update(PackageURL), updated_package_urls)

                # 3. Commit all changes
                session.commit()
                self.logger.log(
                    f"Successfully ingested {len(diffs)} diffs using batched approach."
                )
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

        # note that resolved_urls now has the correct URL map for this particular
        # package
        resolved_urls = diff.diff_url(pkg, new_urls)

        if config.exec_config.test and i > 10:
            break

    # send to loader
    # db.ingest(diffs)


if __name__ == "__main__":
    config = Config(PackageManager.HOMEBREW)
    db = HomebrewDB("homebrew_db_main", config)
    main(config, db)
