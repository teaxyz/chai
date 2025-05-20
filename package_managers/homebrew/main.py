#! /usr/bin/env pkgx +python@3.11 uv run

import re
from dataclasses import dataclass
from typing import Dict, List, Optional, Set, Tuple
from uuid import UUID, uuid4

from permalint import normalize_url
from requests import get
from sqlalchemy import select

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


@dataclass
class CurrentGraph:
    package_map: Dict[str, Package]
    dependencies: Dict[UUID, Set[LegacyDependency]]


@dataclass
class Diff:
    new_package: Optional[Package]
    new_urls: Optional[Set[URL]]
    new_dependencies: Optional[Set[LegacyDependency]]
    new_package_urls: Optional[Set[PackageURL]]
    removed_dependencies: Optional[Set[LegacyDependency]]
    updated_package: Optional[Package]
    updated_package_urls: Optional[Set[PackageURL]]


class HomebrewDB(DB):
    def __init__(self, logger_name: str, config: Config):
        super().__init__(logger_name)
        self.config = config
        self.cache: CurrentGraph = self.current_graph()

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

        self.logger.debug(f"{len(package_map)} packages")
        self.logger.debug(f"{len(dependencies)} dependencies")

        return CurrentGraph(package_map=package_map, dependencies=dependencies)

    def set_current_urls(self, urls: CurrentURLs) -> None:
        """Wrapper for setting current urls"""
        self.current_urls: CurrentURLs = self.get_current_urls(urls)

    def diff_pkg(self, pkg: Actual) -> Diff:
        """Wrapper for getting diffs"""
        self.logger.debug(f"Diffing {pkg.formula}")

        diff_result = Diff(
            new_package=None,
            new_urls=set(),
            new_dependencies=set(),
            new_package_urls=set(),
            removed_dependencies=set(),
            updated_package=None,
            updated_package_urls=set(),
        )

        # first, is this a new package or existing?
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
                self.logger.debug(f"Description changed for {pkg.formula}")
        else:
            self.logger.debug(f"Package {pkg.formula} is new")
            new = Package(
                name=pkg.formula,
                package_manager_id=self.config.pm_config.pm_id,
            )
            diff_result.new_package = new
            pkg_id = new.id

        # second, let's work on the URLs
        urls: Tuple[str, UUID] = (
            (pkg.homepage, self.config.url_types.homepage),
            (pkg.source, self.config.url_types.source),
            (pkg.repository, self.config.url_types.repository),
        )
        # above are the actual URLs we need
        # let's see if they exist in the current URLs

        # ok, so these are the package url records for that package
        package_urls: Set[PackageURL] = self.current_urls.package_urls.get(
            pkg_id, set()
        )
        # and here are the actual URL IDs linked to that package
        linked_urls: Set[UUID] = set(pu.url_id for pu in package_urls)

        for url, url_type in urls:
            if not url:  # skip None
                continue

            url_map_key = (url, url_type)
            existing_url = self.current_urls.url_map.get(url_map_key)
            url_id: UUID

            if existing_url:
                url_id = existing_url.id
                self.logger.debug(f"URL {url} for {url_type} already exists")
            else:
                # ok, so the url / url_type combo is new
                # to avoid duplicates, wanna make sure I avoid creating this URL in
                # *this diff_pkg run*
                already_tracked = next(
                    (
                        u
                        for u in diff_result.new_urls
                        if u.url == url and u.url_type_id == url_type
                    ),
                    None,
                )
                if already_tracked:
                    url_id = already_tracked.id
                    self.logger.debug(f"URL {url} for {url_type} already tracked")
                else:
                    self.logger.debug(f"URL {url} for {url_type} is entirely new")
                    # ok, so this is a new URL that I've never seen before
                    new_url = URL(url=url, url_type_id=url_type)
                    diff_result.new_urls.add(new_url)
                    url_id = new_url.id

            # now, check if the pkg_id is linked to url_id
            if url_id not in linked_urls:
                new_pkg_url = PackageURL(id=uuid4(), package_id=pkg_id, url_id=url_id)
                diff_result.new_package_urls.add(new_pkg_url)
                self.logger.debug(f"New package URL {url} for {url_type}")
            else:
                # the link exists. let's update it!
                existing_pkg_url = next(
                    pu for pu in package_urls if pu.url_id == url_id
                )
                diff_result.updated_package_urls.add(existing_pkg_url)
                self.logger.debug(f"Updated package URL {url} for {url_type}")

        # TODO; dependencies
        # any new dependencies?
        # this would depend on whether it's new (all would be new)
        # or not, only ones that changed would be new / remove
        # also, we need to look at all the dependency types
        # also, if it's a new package depending on a new package, it might
        # be a situation, since the new dependency package won't be in the cache
        # but, we can always have a thing that either gets a package id or makes one


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
        # create temp vars for stuff we transform...basically URL
        homepage = normalize_url(formula["homepage"])
        source = normalize_url(formula["urls"]["stable"]["url"])

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
    logger.log("Retrieved Homebrew")

    # get the URLs & set that
    brew_urls = set(brew.source for brew in brew) | set(brew.homepage for brew in brew)
    db.set_current_urls(brew_urls)
    logger.log("Set current URLs")

    # get the diffs
    diffs = []
    for i, actual in enumerate(brew):
        diffs.append(db.diff_pkg(actual))

        if config.exec_config.test and i > 10:
            break

    # send to loader
    # db.load(diffs)


if __name__ == "__main__":
    config = Config(PackageManager.HOMEBREW)
    db = HomebrewDB("homebrew_db_main", config)
    main(config, db)
