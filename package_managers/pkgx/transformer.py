#! /usr/bin/env pkgx +python@3.12 uv run
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Set

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
        self.logger.debug(f"{import_id}: generating URLs")
        final_urls: Set[URL] = set()
        # first collect all the URLs from package.yml
        # then, check the DB to see if we have a canon
        # if yes, proceed with that! otherwise, do the below logic
        db = DB("pkgx_transformer_db_logger")
        urls: List[str] = self.guess(db, import_id)

        self.logger.debug(f"{import_id}: got these possible {urls}")

        # ok, so if we have a homepage
        # let's see if we find it in pkgx's list of URLs
        # if yes, we have a homepage and a canonical URL!
        # let's make sure we cleanse it (we'll do that for everything else)
        # and then let's go through the rest of the logic

        raise ValueError("Not implemented")

        # Source URL for pkgx always comes from distributable.url
        # note that while the staking app can't register non-GitHub URLs, we can still
        # clean and load them.
        # for now, we're just returning the raw distributable URL as the source URL for
        # Non-GitHub URLs.
        raw_source_urls = pkgx_package.distributable
        if not isinstance(raw_source_urls, list):
            print(pkgx_package)
            print(pkgx_package.distributable)
            print(type(pkgx_package.distributable))
            raise ValueError(f"Distributable is not a list: {raw_source_urls}")
        for raw_distributable in raw_source_urls:
            clean_source_url = self.canonicalize(raw_distributable.url)
            if clean_source_url:
                source_url = URL(
                    url=clean_source_url,
                    url_type_id=self.url_types.source,
                )
                urls.add(source_url)

            # if the source URL is a GitHub URL, we can also populate the repository URL
            if self.is_github(clean_source_url):
                clean_repository_url = clean_source_url  # already clean
                repository_url = URL(
                    url=clean_repository_url,
                    url_type_id=self.url_types.repository,
                )
                urls.add(repository_url)

        # Homepage URL
        # Homepage comes from the pkgxdev/www repo
        # The API https://pkgx.dev/pkgs/{name}.json returns a blob which may contain
        # the homepage field
        homepage_url: str = ""
        response: Response = get(HOMEPAGE_URL.format(name=import_id))
        if response.status_code == 200:
            data: Dict[str, Any] = response.json()
            if "homepage" in data:
                homepage_url = self.canonicalize(data["homepage"])
                self.logger.debug(f"***** www Endpoint: {import_id}: {homepage_url}")
                homepage_url = URL(
                    url=homepage_url, url_type_id=self.url_types.homepage
                )
                urls.add(homepage_url)

        # if the above didn't work, either because of a bad response or no homepage
        # metadata, then we can try and workout what the homepage is using pkgx's naming
        # convention
        if not homepage_url:
            # if the name doesn't have a slash, then it's a single package named with
            # its homepage
            if not re.search(r"/", import_id):
                self.logger.debug(f"***** CONDITION 1: {import_id}")
                homepage_url = import_id
            # this one is probably homepage/packages/name-of-package
            # or 2 slashes, libstd
            # meaning it's probably a valid homepage as well
            elif re.search(r"/./", import_id):
                self.logger.debug(f"***** CONDITION 2: {import_id}")
                homepage_url = import_id
            # now, some known exceptions:
            # these are generally of the form folder/packages
            # so, single slash
            # everything that is crates.io/{crate}, should be crates.io/crates/{crate}
            # Note that most of these are in Homebrew / Debian
            # ideally, we can just search for them, and pass all the URLs we've got
            # guess_canonicalize_url
            elif re.search(r"^crates.io", import_id):
                self.logger.debug(f"***** CONDITION 3: {import_id}")
                name = import_id.split("/")[-1]
                homepage_url = f"`https://crates.io/crates/{name}"
            elif re.search(r"^mozilla.org", import_id):
                self.logger.debug(f"***** CONDITION 4: {import_id}")
                # for mozilla, nss and nspr are special
                if "nss" in import_id:
                    homepage_url = "https://firefox-source-docs.mozilla.org/security/nss/index.html"
                elif "nspr" in import_id:
                    homepage_url = "http://www.mozilla.org/projects/nspr/"
                else:
                    name = import_id.split("/")[-1]
                    homepage_url = f"github.com/mozilla/{name}"
            elif re.search(r"^poppler.freedesktop.org", import_id):
                self.logger.debug(f"***** CONDITION 5: {import_id}")
                homepage_url = "https://poppler.freedesktop.org/"
            elif re.search(r"^x.org", import_id):
                # they are all one package according to Homebrew and Debian
                homepage_url = "https://www.x.org"
            elif re.search(r"^certifi.io", import_id):
                self.logger.debug(f"***** CONDITION 6: {import_id}")
                homepage_url = "github.com/certifi/python-certifi"
            # this would be suspect
            elif re.search(r"^hdfgroup.org/HDF5", import_id):
                self.logger.debug(f"***** CONDITION 7: {import_id}")
                homepage_url = import_id
            elif re.search(r"^taku910.github.io", import_id):
                self.logger.debug(f"***** CONDITION 8: {import_id}")
                homepage_url = import_id
            else:
                raise ValueError(f"Unknown homepage for {import_id}")

            homepage_url = URL(url=homepage_url, url_type_id=self.url_types.homepage)
            urls.add(homepage_url)

        return list(urls)

    def generate_chai_dependency(self, pkgx_package: PkgxPackage) -> Dependencies:
        return Dependencies(
            build=pkgx_package.build.dependencies,
            test=pkgx_package.test.dependencies,
            dependencies=pkgx_package.dependencies,
        )

    def is_github(self, url: str) -> bool:
        return re.search(GITHUB_PATTERN, url) is not None
