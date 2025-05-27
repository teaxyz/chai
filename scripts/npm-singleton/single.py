#!/usr/bin/env pkgx +python@3.11 uv run --with requests==2.31.0 --with permalint==0.1.8
import argparse
from datetime import datetime
from typing import Any, List, Tuple
from uuid import UUID, uuid4

import requests
from permalint import normalize_url

from core.config import Config, PackageManager
from core.db import DB
from core.models import URL, LegacyDependency, Package, PackageURL

NPM_API_URL = "https://registry.npmjs.org/{name}"


class ChaiDB(DB):
    def __init__(self):
        super().__init__("chai-singleton")

    def check_package_exists(self, pkg_name: str) -> bool:
        with self.session() as session:
            return (
                session.query(Package).filter(Package.name == pkg_name).first()
                is not None
            )

    def get_package_by_derived_id(self, derived_id: str) -> Package:
        with self.session() as session:
            return (
                session.query(Package).filter(Package.derived_id == derived_id).first()
            )

    def load(
        self,
        pkg: Package,
        urls: list[URL],
        runtime_deps: list[LegacyDependency],
        dev_deps: list[LegacyDependency],
    ) -> None:
        """Load a package and its URLs into the database. Uses the same session to avoid
        transactional inconsistencies.

        Args:
            pkg: The package to load.
            urls: The URLs to load.
        """
        with self.session() as session:
            # Load the package first
            session.add(pkg)
            session.flush()  # to create the id
            pkg_id = pkg.id

            # Load the URLs
            for url in urls:
                session.add(url)
            session.flush()  # to create the id
            url_ids = [url.id for url in urls]

            # Create the package URL relationships
            for url_id in url_ids:
                session.add(PackageURL(package_id=pkg_id, url_id=url_id))

            # Create the legacy dependencies
            for dep in runtime_deps:
                session.add(dep)
            for dep in dev_deps:
                session.add(dep)
            session.commit()


def get_package_info(npm_package: str) -> dict[str, Any]:
    url = NPM_API_URL.format(name=npm_package)
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"Failed to get URLs for {npm_package}")
    return response.json()


def get_homepage(package_info: dict) -> str:
    try:
        return canonicalize(package_info["homepage"])
    except KeyError as ke:
        raise Exception(f"Couldn't find homepage in {package_info}: {ke}")


def get_repository_url(package_info: dict) -> str:
    try:
        return canonicalize(package_info["repository"]["url"])
    except KeyError as ke:
        raise Exception(f"Couldn't find repository URL in {package_info}: {ke}")


def get_source_url(package_info: dict) -> str:
    try:
        repository_obj = package_info["repository"]
        if repository_obj["type"] == "git":
            return canonicalize(repository_obj["url"])
        else:
            raise Exception(f"Repository is not a git URL: {repository_obj}")
    except KeyError as ke:
        raise Exception(f"Couldn't find source URL in {package_info}: {ke}")


def canonicalize(url: str) -> str:
    return normalize_url(url)


def get_latest_version(package_info: dict) -> str:
    try:
        dist_tags = package_info["dist-tags"]
        return dist_tags["latest"]
    except KeyError as ke:
        raise Exception(f"Couldn't find latest version in {package_info}: {ke}")


def get_latest_version_dependencies(latest_version: dict) -> dict[str, str]:
    """Gets the dependencies from a version object from NPM's Registry API

    Returns:
      - a dictionary keyed by dependency, with semver range as the value"""
    try:
        return latest_version["dependencies"]
    except KeyError as ke:
        raise Exception(
            f"Couldn't find latest version dependencies in {latest_version}: {ke}"
        )


def get_latest_version_dev_dependencies(latest_version: dict) -> dict[str, str]:
    """Gets the development dependencies from a version object from NPM's Registry API

    Returns:
      - a dictionary keyed by dependency, with semver range as the value"""
    try:
        return latest_version["devDependencies"]
    except KeyError as ke:
        raise Exception(
            f"Couldn't find latest version dev dependencies in {latest_version}: {ke}"
        )


def get_urls(package_info: dict) -> Tuple[str, str, str]:
    homepage = get_homepage(package_info)
    repository_url = get_repository_url(package_info)
    source_url = get_source_url(package_info)
    return homepage, repository_url, source_url


def generate_url(url_type_id: UUID, url: str) -> URL:
    return URL(id=uuid4(), url=url, url_type_id=url_type_id)


def generate_legacy_dependencies(
    db: ChaiDB, pkg: Package, deps: dict[str, str], dependency_type_id: UUID
) -> list[LegacyDependency]:
    legacy_deps: list[LegacyDependency] = []

    for dep_name, dep_range in deps.items():
        derived_id = f"npm/{dep_name}"
        chai_dep: Package | None = db.get_package_by_derived_id(derived_id)

        if not chai_dep:
            print(f"Dependency {dep_name} does not exist in CHAI...skipping")
            continue

        dependency = LegacyDependency(
            package_id=pkg.id,
            dependency_id=chai_dep.id,
            dependency_type_id=dependency_type_id,
            semver_range=dep_range,
        )
        legacy_deps.append(dependency)

    return legacy_deps


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Load a single NPM package by name into CHAI"
    )
    parser.add_argument("name", help="Name of the NPM package")
    args = parser.parse_args()

    config = Config(PackageManager.NPM)

    chai_db = ChaiDB()
    if chai_db.check_package_exists(args.name):
        print(f"Package {args.name} already exists")
        exit(1)

    # Establish now for the created_at
    now = datetime.now()

    # Get Package Info from NPM
    package_info = get_package_info(args.name)

    # Create Package
    name = args.name
    derived_id = f"npm/{name}"
    package_manager_id = config.pm_config.pm_id
    import_id = f"npm-singleton/{name}"
    readme = package_info["readme"]

    pkg = Package(
        id=uuid4(),
        name=name,
        derived_id=derived_id,
        package_manager_id=package_manager_id,
        import_id=import_id,
        readme=readme,
    )

    # URLs
    homepage, repository, source = get_urls(package_info)

    homepage_url = generate_url(config.url_types.homepage, normalize_url(homepage))
    repository_url = generate_url(
        config.url_types.repository, normalize_url(repository)
    )
    source_url = generate_url(config.url_types.source, normalize_url(source))

    urls = [homepage_url, repository_url, source_url]

    # Dependencies
    latest_version = get_latest_version(package_info)
    latest_version_info = package_info["versions"][latest_version]
    deps = get_latest_version_dependencies(latest_version_info)
    dev_deps = get_latest_version_dev_dependencies(latest_version_info)

    runtime_deps: list[LegacyDependency] = generate_legacy_dependencies(
        chai_db, pkg, deps, config.dependency_types.runtime
    )
    dev_deps: list[LegacyDependency] = generate_legacy_dependencies(
        chai_db, pkg, dev_deps, config.dependency_types.development
    )
    # Load the package and its URLs into the database
    chai_db.load(pkg, urls, runtime_deps, dev_deps)
