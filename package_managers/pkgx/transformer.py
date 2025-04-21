#! /usr/bin/env pkgx +python@3.12 uv run
import re
from dataclasses import dataclass, field
from typing import Dict, List, Set, Tuple

from core.config import Config, URLTypes
from core.models import URL, LegacyDependency, Package
from core.transformer import Transformer
from package_managers.pkgx.parser import PkgxPackage

# for pkgx, we need to create the following:
# - packages
# = package dependencies (not version to packages) --> legacy dependencies
# = urls
# that's all.

# we'd need a cache where we store the identifiers, to the package

GITHUB_PATTERN = r"(https://github\.com/[^/]+/[^/]+)"


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
        self.cache_map: Dict[str, Package] = {}

    # The parser is yielding one package at a time
    def transform(self, project_path: str, data: PkgxPackage) -> None:
        item = Cache()

        import_id = project_path
        item.package = self.generate_chai_package(import_id)
        item.urls = self.generate_chai_url(data)
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

    def generate_chai_url(self, pkgx_package: PkgxPackage) -> List[URL]:
        urls: Set[URL] = set()

        # Source URL comes from the distributable object, and a package
        # can have multiple distributable objects
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

        # Homepage URL
        # Homepage comes from the versions object
        versions = pkgx_package.versions
        if versions.github:
            owner_repo = self.remove_tags_releases(versions.github)
            raw_homepage_url = f"https://github.com/{owner_repo}"
            homepage_url = URL(
                url=raw_homepage_url,
                url_type_id=self.url_types.homepage,
            )
            urls.add(homepage_url)
        if versions.gitlab:
            owner_repo = self.remove_tags_releases(versions.gitlab)
            raw_homepage_url = f"https://gitlab.com/{owner_repo}"
            homepage_url = URL(
                url=raw_homepage_url,
                url_type_id=self.url_types.homepage,
            )
            urls.add(homepage_url)
        if versions.url:
            raw_homepage_url = versions.url
            homepage_url = URL(
                url=raw_homepage_url,
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

        # TODO: implement distributable URL patterns
        # if self.is_distributable_url(url):
        #     return self.extract_distributable_url(url)

        return None

    def is_github(self, url: str) -> bool:
        return re.match(GITHUB_PATTERN, url) is not None

    def extract_github_repo(self, url: str) -> str:
        return re.match(GITHUB_PATTERN, url).group(1)

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
