#! /usr/bin/env pkgx +python@3.11 uv run

import re
from dataclasses import dataclass
from typing import Dict, List, Optional, Set
from uuid import UUID, uuid4

from permalint import canonicalize
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

    def diff_pkg(self, pkg: Actual) -> List[Diff]:
        """Wrapper for getting diffs"""
        # new package?
        new_package: Optional[Package] = None
        if pkg.name not in self.cache.package_map:
            new_package = Package(
                id=uuid4(),
                derived_id=f"homebrew/{pkg.name}",
                name=pkg.name,
                readme=pkg.description,
                import_id=pkg.name,
                package_manager_id=self.config.pm_config.pm_id,
            )
            pkg_id = new_package.id
        else:
            pkg_id = self.cache.package_map[pkg.name].id

        # so, we have a package ID.
        # we can also use new_package to denote if it is new or not

        # any new URLs?
        new_urls: Dict[str, URL] = {}

        # check if in the url map
        # if not, add it, and then also grab the ID
        # if it is, just grab the ID
        if pkg.homepage not in self.current_urls.url_map:
            new_urls[pkg.homepage] = URL(
                id=uuid4(), url=pkg.homepage, url_type_id=config.url_types.homepage
            )
            homepage_url_id = new_urls[pkg.homepage].id
        else:
            homepage_url_id = self.current_urls.url_map[pkg.homepage].id

        if pkg.source not in self.current_urls.url_map:
            new_urls[pkg.source] = URL(
                id=uuid4(), url=pkg.source, url_type_id=config.url_types.source
            )
            source_url_id = new_urls[pkg.source].id
        else:
            source_url_id = self.current_urls.url_map[pkg.source].id

        if pkg.repository not in self.current_urls.url_map:
            new_urls[pkg.repository] = URL(
                id=uuid4(),
                url=pkg.repository,
                url_type_id=config.url_types.repository,
            )
            repository_url_id = new_urls[pkg.repository].id
        else:
            repository_url_id = self.current_urls.url_map[pkg.repository].id

        actual_linked_urls: Set[UUID] = set(
            [homepage_url_id, source_url_id, repository_url_id]
        )

        # any new Package-URLs?
        new_package_urls: Set[PackageURL] = set()

        # if it's new, then everything will be new
        if new_package:
            for url in new_urls.values():
                new_package_urls.add(
                    PackageURL(
                        id=uuid4(),
                        package_id=new_package.id,
                        url_id=url.id,
                    )
                )
        else:
            current_package_urls = self.current_urls.package_urls[pkg_id]
            current_linked_urls: Set[UUID] = set(
                [item.url_id for item in current_package_urls]
            )
            url_diff = actual_linked_urls - current_linked_urls
            if url_diff:
                for url in url_diff:
                    new_package_urls.add(
                        PackageURL(id=uuid4(), package_id=pkg_id, url_id=url)
                    )

        # dependencies
        current_deps: Optional[Set[LegacyDependency]] = self.cache.dependencies.get(
            pkg.id, None
        )
        if current_deps:
            current_deps = {(dep.package_id, dep.dependency_id) for dep in current_deps}

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
        homepage = canonicalize(formula["homepage"])
        source = canonicalize(formula["urls"]["stable"])

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
    brew_urls = set(brew.source for brew in brew) + set(brew.homepage for brew in brew)
    current_urls = db.set_current_urls(brew_urls)
    logger.log("Set current URLs")


if __name__ == "__main__":
    config = Config(PackageManager.HOMEBREW)
    db = HomebrewDB("homebrew_db_main", config)
    main(config, db)
