#!/usr/bin/env pkgx +python@3.11 uv run --with requests==2.31.0
import argparse
from typing import Any, Tuple

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

    def load_package(self, pkg: Package):
        with self.session() as session:
            session.add(pkg)
            session.commit()

    def load_url(self, url: URL):
        with self.session() as session:
            session.add(url)
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
    print(pkg.to_dict())

    # URLs
    homepage, repository, source = get_urls(package_info)

    homepage_url = URL(url=homepage, url_type_id=config.url_types.homepage)
    repository_url = URL(url=repository, url_type_id=config.url_types.repository)
    source_url = URL(url=source, url_type_id=config.url_types.source)

    print(homepage_url.to_dict())
    print(repository_url.to_dict())
    print(source_url.to_dict())

    chai_db.load_package(pkg)
    chai_db.load_url(homepage_url)
    chai_db.load_url(repository_url)
    chai_db.load_url(source_url)
