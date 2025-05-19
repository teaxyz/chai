#! /usr/bin/env pkgx +python@3.12 uv run
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set

from requests import Response, get

from core.config import Config, URLTypes
from core.db import DB
from core.models import URL, Package
from core.transformer import Transformer
from package_managers.pkgx.parser import PkgxPackage

# for pkgx, we need to create the following:
# - packages
# = package dependencies (not version to packages) --> legacy dependencies
# = urls
# that's all.

# we'd need a cache where we store the identifiers, to the package

GITHUB_PATTERN = r"github\.com/[^/]+/[^/]+"
HOMEPAGE_URL = "https://pkgx.dev/pkgs/{name}.json"


@dataclass
class Dependencies:
    build: list[str] = field(default_factory=list[str])
    test: list[str] = field(default_factory=list[str])
    dependencies: list[str] = field(default_factory=list[str])


@dataclass
class Cache:
    package: Package = field(default_factory=Package)
    urls: list[URL] = field(default_factory=list[URL])
    dependencies: Dependencies = field(default_factory=Dependencies)


class PkgxTransformer(Transformer):
    def __init__(self, config: Config, db: DB):
        super().__init__("pkgx_transformer")
        self.package_manager_id = config.pm_config.pm_id
        self.url_types: URLTypes = config.url_types
        self.depends_on_types = config.dependency_types
        self.cache_map: Dict[str, Cache] = {}
        self.db = db
        self.package_managers = config.package_managers

    # The parser is yielding one package at a time
    def transform(self, project_path: str, data: PkgxPackage) -> None:
        item: Cache = Cache()

        import_id = project_path
        item.package = self.generate_chai_package(import_id)
        item.urls = self.generate_chai_url(import_id, data)
        item.dependencies = self.generate_chai_dependency(data)

        # add it to the cache
        self.cache_map[project_path] = item
        self.cache_map[import_id] = item

    def generate_chai_package(self, import_id: str) -> Package:
        derived_id = f"pkgx/{import_id}"
        name = import_id
        package = Package(
            derived_id=derived_id,
            name=name,
            package_manager_id=self.package_manager_id,
            import_id=import_id,
        )

        # add it to the cache
        cache = Cache(package=package)
        self.cache_map[import_id] = cache

        return package

    def ask_pkgx(self, import_id: str) -> Optional[str]:
        """
        ask max's scraping work for the homepage of a package
        Homepage comes from the pkgxdev/www repo
        The API https://pkgx.dev/pkgs/{name}.json returns a blob which may contain
        the homepage field
        """
        response: Response = get(HOMEPAGE_URL.format(name=import_id))
        if response.status_code == 200:
            data: Dict[str, Any] = response.json()
            if "homepage" in data:
                return data["homepage"]

    def special_case(self, import_id: str) -> Optional[str]:
        homepage: Optional[str] = None

        # if no slashes, then pkgx used the homepage as the name
        # if two slashes, then probably github / gitlab
        if not re.search(r"/", import_id) or re.search(r"/.+/", import_id):
            homepage = import_id

        # if it's a crates.io package, then we can use the crates URL
        elif re.search(r"^crates.io", import_id):
            homepage = f"https://crates.io/crates/{import_id}"

        # if it's part of the x.org family
        elif re.search(r"^x.org", import_id):
            homepage = "https://x.org"

        # if it's part of the pkgx family
        elif re.search("^pkgx.sh", import_id):
            tool = import_id.split("/")[1]
            homepage = f"https://github.com/pkgxdev/{tool}"

        else:
            self.logger.warn(f"no homepage in pkgx for {import_id}")

        return homepage

    def generate_chai_url(self, import_id: str, pkgx_package: PkgxPackage) -> List[URL]:
        urls: Set[URL] = set()

        # handle homepage first

        # get possible homepage URLs
        maybe: List[str] = self.guess(
            self.db,
            import_id,
            [self.package_managers.homebrew, self.package_managers.debian],
        )

        # if we have a canonical URL, we can proceed with that!
        if maybe:
            # NOTE: why am I using the first one?
            # well, permalint arranges the URLs in order of preference, so i'm relying
            # on that for now
            homepage = maybe[0]
        else:
            # otherwise, we can ask pkgx
            homepage = self.ask_pkgx(import_id)

            # but if that fails, we're in special case territory
            if not homepage:
                homepage = self.special_case(import_id)

        if homepage:
            # finally, canonicalize it
            homepage = self.canonicalize(homepage)
            urls.add(URL(url=homepage, url_type_id=self.url_types.homepage))

        # Source URL for pkgx always comes from distributable.url
        # which our parser returns in list form
        # we'll clean and load em all
        for raw_distributable in pkgx_package.distributable:
            clean = self.canonicalize(raw_distributable.url)
            if clean:
                source = URL(url=clean, url_type_id=self.url_types.source)
                urls.add(source)

            # if the source URL is a GitHub URL, we can also populate the repository URL
            if self.is_github(clean):
                repository = URL(url=clean, url_type_id=self.url_types.repository)
                urls.add(repository)

        return list(urls)

    def generate_chai_dependency(self, pkgx_package: PkgxPackage) -> Dependencies:
        return Dependencies(
            build=pkgx_package.build.dependencies,
            test=pkgx_package.test.dependencies,
            dependencies=pkgx_package.dependencies,
        )

    def is_github(self, url: str) -> bool:
        return re.search(GITHUB_PATTERN, url) is not None
