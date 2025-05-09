#! /usr/bin/env pkgx +python@3.12 uv run
import re
from dataclasses import dataclass, field
from typing import Dict, List, Set

from requests import get

from core.config import Config, URLTypes
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
    def __init__(self, config: Config):
        super().__init__("pkgx_transformer")
        self.package_manager_id = config.pm_config.pm_id
        self.url_types: URLTypes = config.url_types
        self.depends_on_types = config.dependency_types
        self.cache_map: Dict[str, Cache] = {}

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

    def generate_chai_url(self, import_id: str, pkgx_package: PkgxPackage) -> List[URL]:
        urls: Set[URL] = set()

        # Source URL for pkgx always comes from distributable.url
        # Note that while the staking app can't register non-GitHub URLs, we can still
        # clean and load them.
        # For now, we're just returning the raw distributable URL as the source URL for
        # Non-GitHub URLs.
        if isinstance(pkgx_package.distributable, list):
            for distributable in pkgx_package.distributable:
                raw_source_url = distributable.url
                clean_source_url = self.clean_distributable_url(raw_source_url)
                if clean_source_url:
                    source_url = URL(
                        url=clean_source_url,
                        url_type_id=self.url_types.source,
                    )
                    urls.add(source_url)
        else:
            raw_source_url = pkgx_package.distributable.url

        clean_source_url = self.clean_distributable_url(raw_source_url)
        if clean_source_url:
            source_url = URL(
                url=clean_source_url,
                url_type_id=self.url_types.source,
            )
            urls.add(source_url)

        # Repository URL
        if self.is_github(raw_source_url):
            raw_repository_url = self.extract_github_repo(raw_source_url)
            repository_url = URL(
                url=raw_repository_url,
                url_type_id=self.url_types.repository,
            )
            urls.add(repository_url)
        self.logger.log(f"***** {repository_url.url}")

        # Homepage URL
        # Homepage comes from the pkgxdev/www repo
        # The API https://pkgx.dev/pkgs/{name}.json returns a blob which may contain
        # the homepage field
        response = get(HOMEPAGE_URL.format(name=import_id))
        if response.status_code == 200:
            data = response.json()
            if "homepage" in data:
                homepage_url = URL(
                    url=data["homepage"],
                    url_type_id=self.url_types.homepage,
                )
                urls.add(homepage_url)

        return list(urls)

    def generate_chai_dependency(self, pkgx_package: PkgxPackage) -> Dependencies:
        return Dependencies(
            build=pkgx_package.build.dependencies,
            test=pkgx_package.test.dependencies,
            dependencies=pkgx_package.dependencies,
        )

    def clean_distributable_url(self, url: str) -> str:
        # if the URL matches a GitHub tarball, use the repo as the source URL
        if self.is_github(url):
            return self.extract_github_repo(url)
        else:
            # TODO: implement different URL patterns
            return url

    def is_github(self, url: str) -> bool:
        return re.search(GITHUB_PATTERN, url) is not None

    def extract_github_repo(self, url: str) -> str:
        return re.search(GITHUB_PATTERN, url).group(0)

    def is_distributable_url(self, url: str) -> bool:
        # https://archive.mozilla.org/pub/nspr/releases/v{{version}}/src/nspr-{{version}}.tar.gz
        return re.match(r"https://(.*)/?v{{version}}", url) is not None

    def extract_distributable_url(self, url: str) -> str:
        return re.match(r"https://(.*)/?v{{version}}", url).group(1)

    def remove_tags_releases(self, url: str) -> str:
        """Sometimes, the versions object is owner/repo/tags or owner/repo/releases
        This functions removes tags or releases from the URL"""
        if "tags" in url:
            return re.sub(r"/tags$", "", url)
        if "releases" in url:
            return re.sub(r"/releases$", "", url)
        return url
