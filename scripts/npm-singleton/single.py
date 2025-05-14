#!/usr/bin/env pkgx +python@3.11 uv run --with requests==2.31.0
import argparse
from typing import Any, List, Tuple

import requests

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

    def load(self, pkg: Package, urls: List[URL]) -> None:
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
            session.commit()


def get_package_info(npm_package: str) -> dict[str, Any]:
    url = NPM_API_URL.format(name=npm_package)
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"Failed to get URLs for {npm_package}")
    return response.json()


def get_homepage(package_info: dict) -> str:
    try:
        return package_info["homepage"]
    except KeyError as ke:
        raise Exception(f"Couldn't find homepage in {package_info}: {ke}")


def get_repository_url(package_info: dict) -> str:
    try:
        return package_info["repository"]["url"]
    except KeyError as ke:
        raise Exception(f"Couldn't find repository URL in {package_info}: {ke}")


def get_source_url(package_info: dict) -> str:
    try:
        repository_obj = package_info["repository"]
        if repository_obj["type"] == "git":
            return repository_obj["url"]
        else:
            raise Exception(f"Repository is not a git URL: {repository_obj}")
    except KeyError as ke:
        raise Exception(f"Couldn't find source URL in {package_info}: {ke}")


def get_urls(package_info: dict) -> Tuple[str, str, str]:
    homepage = get_homepage(package_info)
    repository_url = get_repository_url(package_info)
    source_url = get_source_url(package_info)
    return homepage, repository_url, source_url


# TODO: Implement this
def get_dependencies(package_info: dict) -> list[LegacyDependency]:
    pass


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

    # Get Package Info from NPM
    package_info = get_package_info(args.name)

    # Create Package
    name = args.name
    derived_id = f"npm/{name}"
    package_manager_id = config.pm_config.pm_id
    import_id = f"npm-singleton/{name}"
    readme = package_info["readme"]

    pkg = Package(
        name=name,
        derived_id=derived_id,
        package_manager_id=package_manager_id,
        import_id=import_id,
        readme=readme,
    )

    # URLs
    homepage, repository, source = get_urls(package_info)

    homepage_url = URL(url=homepage, url_type_id=config.url_types.homepage)
    repository_url = URL(url=repository, url_type_id=config.url_types.repository)
    source_url = URL(url=source, url_type_id=config.url_types.source)

    urls = [homepage_url, repository_url, source_url]

    chai_db.load(pkg, urls)
