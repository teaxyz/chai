import csv
from collections.abc import Generator
from datetime import datetime
from uuid import UUID, uuid4

from core.config import Config, PackageManager
from core.db import DB
from core.fetcher import TarballFetcher
from core.logger import Logger
from core.models import URL, LegacyDependency, Package, PackageURL
from core.structs import Cache, CurrentGraph, CurrentURLs, URLKey
from core.transformer import Transformer
from core.utils import is_github_url
from package_managers.crates.structs import (
    Crate,
    CrateDependency,
    CrateLatestVersion,
    CrateUser,
    DependencyType,
)


class CratesDB(DB):
    def __init__(self, config: Config):
        super().__init__("crates_db")
        self.config = config
        # self.set_current_graph()

    def set_current_graph(self) -> None:
        self.graph: CurrentGraph = self.current_graph(self.config.pm_config.pm_id)

    def set_current_urls(self, urls: set[str]) -> None:
        self.urls: CurrentURLs = self.current_urls(urls)


class CratesTransformer(Transformer):
    def __init__(self, config: Config):
        super().__init__("crates")
        self.config = config
        self.crates: dict[int, Crate] = {}

        # files we need to parse
        self.files: dict[str, str] = {
            "crates": "crates.csv",
            "latest_versions": "default_versions.csv",
            "versions": "versions.csv",
            "dependencies": "dependencies.csv",
            "users": "users.csv",
            "teams": "teams.csv",
        }

    def _open_csv(self, file_name: str) -> Generator[dict[str, str], None, None]:
        try:
            file_path = self.finder(self.files[file_name])
            with open(file_path, newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    yield row
        except KeyError:
            raise KeyError(f"Missing {file_name} from self.files: {self.files}")
        except FileNotFoundError:
            self.logger.error(f"Missing {file_path} from data directory")
            raise FileNotFoundError(f"Missing {file_path} file")
        except Exception as e:
            self.logger.error(f"Error reading {file_path}: {e}")
            raise e

    def parse(self) -> None:
        # first go through crates.csv to
        # here, we can get the import_id, name, homepage, documentation, repository
        # and also source, from repo if it is like GitHub
        for i, row in enumerate(self._open_csv("crates")):
            crate_id = int(row["id"])
            name = row["name"]
            readme = row["readme"]

            # URLs:
            homepage = self.canonicalize(row["homepage"])
            documentation = self.canonicalize(row["documentation"])
            repository = self.canonicalize(row["repository"])

            source: str | None = None
            if is_github_url(repository):
                source = repository

            crate = Crate(
                crate_id, name, readme, homepage, repository, documentation, source
            )
            self.crates[crate_id] = crate

            if self.config.exec_config.test and i > 100:
                break

        self.logger.log(f"Parsed {len(self.crates)} crates")

        if self.config.exec_config.test:
            self.logger.log("Test mode, only parsing 100 crates")
            return

        # get the map of crate_id to latest_version_id, and the set of all
        # latest_version_ids
        latest_versions: set[int]
        latest_versions_map: dict[int, int]
        latest_versions, latest_versions_map = self._load_latest_versions()
        self.logger.log(f"Loaded {len(latest_versions)} latest versions")

        # also build the map of user_id to CrateUser object
        users: dict[int, CrateUser] = self._load_users()
        self.logger.log(f"Loaded {len(users)} users")

        # now, iterate through the versions.csv, and populate LatestVersion objects,
        # only if the version_id is in the latest_versions set
        for row in self._open_csv("versions"):
            version_id = int(row["id"])
            crate_id = int(row["crate_id"])

            # ignore if this version is not the latest
            if version_id not in latest_versions:
                continue

            if crate_id not in self.crates.keys() and self.config.exec_config.test:
                # in test mode, we only look at a few crates
                continue
            elif crate_id not in self.crates.keys():
                # should never run into this
                raise ValueError(f"Crate {crate_id} not found in self.crates")

            checksum = row["checksum"]
            downloads = int(row["downloads"])
            license = row["license"]
            num = row["num"]
            published_at = row["created_at"]

            # make a CrateUser object from the published_by
            published_by = row["published_by"]
            published_by_user: CrateUser | None = (
                users[int(published_by)] if published_by else None
            )

            latest_version = CrateLatestVersion(
                version_id,
                checksum,
                downloads,
                license,
                num,
                published_by_user,
                published_at,
            )

            # map this LatestVersion to the crate in self.crates
            self.crates[crate_id].latest_version = latest_version

        self.logger.log("Parsed the latest versions for each crate")

        # finally, parse through the dependencies.csv
        # again, we only care about the dependencies for the latest version
        for row in self._open_csv("dependencies"):
            start_id = int(row["version_id"])

            if start_id not in latest_versions:
                continue

            end_crate_id = int(row["crate_id"])

            # we can do the same check for end_id as we do above, for a test mode
            if end_crate_id not in self.crates.keys() and self.config.exec_config.test:
                continue
            elif end_crate_id not in self.crates.keys():
                # again, this should never happen
                raise ValueError(f"Crate {end_crate_id} not found in self.crates")

            start_crate_id = latest_versions_map[start_id]

            if (
                start_crate_id not in self.crates.keys()
                and self.config.exec_config.test
            ):
                continue
            elif start_crate_id not in self.crates.keys():
                # again, this should never happen
                raise ValueError(f"Crate {start_crate_id} not found in self.crates")

            kind = int(row["kind"])

            if kind not in [0, 1, 2]:
                raise ValueError(f"Unknown dependency kind: {kind}")

            dependency_type = DependencyType(kind)
            semver = row["req"]

            dependency = CrateDependency(
                start_crate_id, end_crate_id, dependency_type, semver
            )

            # add this dependency to the crate
            self.crates[start_crate_id].latest_version.dependencies.append(dependency)

        self.logger.log("Parsed the dependencies for each crate")

        # count latest versions and dependencies
        latest_version_count: int = 0
        dependency_count: int = 0
        for crate in self.crates.values():
            if crate.latest_version is not None:
                latest_version_count += 1

                if crate.latest_version.dependencies:
                    dependency_count += 1

        self.logger.log(f"Found {latest_version_count} latest versions")
        self.logger.log(f"Found {dependency_count} dependencies")

    def _load_latest_versions(self) -> tuple[set[int], dict[int, int]]:
        latest_versions: set[int] = set()
        latest_versions_map: dict[int, int] = {}
        for row in self._open_csv("latest_versions"):
            crate_id = int(row["crate_id"])
            version_id = int(row["version_id"])
            latest_versions.add(version_id)
            latest_versions_map[version_id] = crate_id

        return latest_versions, latest_versions_map

    def _load_users(self) -> dict[int, CrateUser]:
        users: dict[int, CrateUser] = {}
        for row in self._open_csv("users"):
            user_id = int(row["id"])
            name = row["name"]
            github_username = row["gh_login"]
            user = CrateUser(user_id, name, github_username)
            users[user_id] = user

        return users


class Diff:
    def __init__(self, config: Config, caches: Cache):
        self.config = config
        self.now = datetime.now()
        self.caches = caches
        self.logger = Logger("crates_diff")

    def diff_pkg(self, pkg: Crate) -> tuple[UUID, Package | None, dict | None]:
        """
        Checks if the given pkg is in the package_cache.

        Returns:
            pkg_id: UUID, the id of the package in the db
            pkg_obj: Package | None, the package object if it's new
            update_payload: dict | None, the update payload if it's an update
        """
        pkg_id: UUID
        crate_id: str = str(pkg.id)  # import_ids are strings in the db
        if crate_id not in self.caches.package_map:
            # new package
            p = Package(
                id=uuid4(),
                derived_id=f"crates/{pkg.name}",
                name=pkg.name,
                package_manager_id=self.config.pm_config.pm_id,
                import_id=crate_id,
                readme=pkg.readme,
                created_at=self.now,
                updated_at=self.now,
            )
            pkg_id = p.id
            return pkg_id, p, {}
        else:
            # it's in the cache, so check for changes
            p = self.caches.package_map[crate_id]
            pkg_id = p.id
            # check for changes
            # right now, that's just the readme
            if p.readme != pkg.readme:
                return (
                    pkg_id,
                    None,
                    {"id": p.id, "readme": pkg.readme, "updated_at": self.now},
                )
            else:
                # existing package, no change
                return pkg_id, None, None

    def diff_url(self, pkg: Crate, new_urls: dict[URLKey, URL]) -> dict[UUID, UUID]:
        """
        Identifies the correct URL for this crate, based on fetched data and all URL
        strings collected so far

        Returns:
            resolved_urls: dict[UUID, UUID], the resolved URL for this crate
        """
        resolved_urls: dict[UUID, UUID] = {}

        urls: list[URLKey] = [
            URLKey(pkg.homepage, self.config.url_types.homepage),
            URLKey(pkg.repository, self.config.url_types.repository),
            URLKey(pkg.documentation, self.config.url_types.documentation),
            URLKey(pkg.source, self.config.url_types.source),
        ]

        for url_key in urls:
            url = url_key.url
            url_type = url_key.url_type_id

            # guard: no URL
            if not url:
                continue

            resolved_url_id: UUID

            if url_key in new_urls:
                # if we've already tried to create this URL, use that one
                resolved_url_id = new_urls[url_key].id
            elif url_key in self.caches.url_map:
                # if it's already in the database, let's use that one
                resolved_url_id = self.caches.url_map[url_key].id
            else:
                # most will be here because it's the first run of clean data
                # BIG HONKING TODO: uncomment this later
                # self.logger.debug(f"URL {url} for {url_type} is entirely new")
                # end of BIG HONKING TODO
                new_url = URL(
                    id=uuid4(),
                    url=url,
                    url_type_id=url_type,
                    created_at=self.now,
                    updated_at=self.now,
                )
                resolved_url_id = new_url.id

                # NOTE: THIS IS SUPER IMPORTANT
                # we're adding to new_urls here, not just in main
                new_urls[url_key] = new_url

            resolved_urls[url_type] = resolved_url_id

        return resolved_urls

    def diff_pkg_url(
        self, pkg_id: UUID, resolved_urls: dict[UUID, UUID]
    ) -> tuple[list[PackageURL], list[dict]]:
        """Takes in a package_id and resolved URLs from diff_url, and generates
        new PackageURL objects as well as a list of changes to existing ones

        Inputs:
          - pkg_id: the id of the package
          - resolved_urls: a map of url types to final URL ID for this pkg

        Outputs:
          - new_package_urls: a list of new PackageURL objects
          - updated_package_urls: a list of changes to existing PackageURL objects

        TODO:
          - We're updating every single package_url entity, which takes time. We should
            check if the latest URL has changed, and if so, only update that one.
        """
        new_links: list[PackageURL] = []
        updates: list[dict] = []

        # what are the existing links?
        existing: set[UUID] = {
            pu.url_id for pu in self.caches.package_urls.get(pkg_id, set())
        }

        # for the correct URL type / URL for this package:
        for url_type, url_id in resolved_urls.items():
            if url_id not in existing:
                # new link!
                new_links.append(
                    PackageURL(
                        id=uuid4(),
                        package_id=pkg_id,
                        url_id=url_id,
                        created_at=self.now,
                        updated_at=self.now,
                    )
                )
            else:
                # TODO: this should only happen for `latest` URLs
                # there is an existing link between this URL and this package
                # let's find it
                existing_pu = next(
                    pu
                    for pu in self.caches.package_url_cache[pkg_id]
                    if pu.url_id == url_id
                )
                existing_pu.updated_at = self.now
                updates.append({"id": existing_pu.id, "updated_at": self.now})

        return new_links, updates

    def diff_deps(
        self, pkg: Crate
    ) -> tuple[list[LegacyDependency], list[LegacyDependency]]:
        """
        Identifies new and removed dependencies for a given crate

        The process is:
           1. Build a view of what the package's dependencies are according to
              the crates.io database.
           2. Get this crate's Package ID from CHAI
           3. Get this crate's existing dependencies from CHAI
           4. Compare the two sets, and identify new and removed dependencies

        Returns:
            new_deps: list[LegacyDependency], the new dependencies
            removed_deps: list[LegacyDependency], the removed dependencies
        """
        new_deps: list[LegacyDependency] = []
        removed_deps: list[LegacyDependency] = []

        actual: set[tuple[UUID, UUID]] = set()

        # first, I need to map the actual dependencies into the
        # (dep_id, dep_type) tuple
        for dependency in pkg.latest_version.dependencies:
            dep_crate_id: str = str(dependency.dependency_id)
            dep_type: DependencyType = dependency.dependency_type

            # guard: no dep_id
            if not dep_crate_id:
                raise ValueError(f"No dep_id for {dependency}")

            # guard: no dep_type
            # can't do if not dep_type bceause IntEnums might be 0, which is False
            if dep_type is None:
                raise ValueError(f"No dep_type for {dependency}")

            # get the ID from the cache
            dependency = self.caches.package_map.get(dep_crate_id)

            # if we don't have the dependency, means there's a package which depends on
            # something we have not indexed yet
            # it's not problem, since we'll load the package now, and on the next run,
            # this will sort itself out
            if not dependency:
                self.logger.warn(f"{dep_crate_id}, dependency of {pkg.name} is new")
                continue

            # figure out the dependency type UUID
            dependency_type = self._resolve_dep_type(dep_type)

            # add it to the set of actual dependencies
            actual.add((dependency.id, dependency_type))

        # establish the package that we are working with
        crate_id: str = str(pkg.id)
        package = self.caches.package_map.get(crate_id)
        if not package:
            # TODO: handle this case, though it fixes itself on the next run
            self.logger.warn(f"New package {pkg.name}, will grab its deps next time")
            return [], []

        pkg_id: UUID = package.id

        # what are its existing dependencies?
        # specifically, existing dependencies IN THE SAME STRUCTURE as `actual`,
        # so we can do an easy comparison
        existing: set[tuple[UUID, UUID]] = {
            (dep.dependency_id, dep.dependency_type_id)
            for dep in self.caches.dependencies.get(pkg_id, set())
        }

        # we have two sets!
        # actual minus existing = new_deps
        # existing minus actual = removed_deps
        new = actual - existing
        removed = existing - actual

        new_deps = [
            LegacyDependency(
                id=uuid4(),
                package_id=pkg_id,
                dependency_id=dep[0],
                dependency_type_id=dep[1],
                created_at=self.now,
                updated_at=self.now,
            )
            for dep in new
        ]

        removed_deps = [
            LegacyDependency(
                id=uuid4(),
                package_id=pkg_id,
                dependency_id=dep[0],
                dependency_type_id=dep[1],
                created_at=self.now,
                updated_at=self.now,
            )
            for dep in removed
        ]

        return new_deps, removed_deps

    def _resolve_dep_type(self, dep_type: DependencyType) -> UUID:
        """
        Resolves the dependency type UUID from the config
        """
        if dep_type == DependencyType.NORMAL:
            return self.config.dependency_types.runtime
        elif dep_type == DependencyType.BUILD:
            return self.config.dependency_types.build
        elif dep_type == DependencyType.DEV:
            return self.config.dependency_types.development
        else:
            raise ValueError(f"Unknown dependency type: {dep_type}")


def main(config: Config, db: CratesDB):
    logger = Logger("crates_main_v2")
    logger.log("Starting crates_main_v2")

    fetcher: TarballFetcher = TarballFetcher(
        "crates",
        config.pm_config.source,
        config.exec_config.no_cache,
        config.exec_config.test,
    )
    if config.exec_config.fetch:
        files = fetcher.fetch()

    if not config.exec_config.no_cache:
        logger.log("Writing files to disk")
        fetcher.write(files)

    transformer = CratesTransformer(config)
    transformer.parse()

    # the transformer object has transformer.crates, which has all the info
    # now, let's build the db's cache
    # we need URLs
    crates_urls: set[str] = set()
    for crate in transformer.crates.values():
        crates_urls.add(crate.homepage)
        crates_urls.add(crate.repository)
        crates_urls.add(crate.documentation)

    logger.log(f"Found {len(crates_urls)} crates URLs")
    db.set_current_urls(crates_urls)

    # now, we can build the cache
    cache = Cache(
        db.graph.package_map,
        db.urls.url_map,
        db.urls.package_urls,
        db.graph.dependencies,
    )

    new_packages: list[Package] = []
    updated_packages: list[dict] = []
    new_urls: dict[URLKey, URL] = {}
    new_package_urls: list[PackageURL] = []
    updated_package_urls: list[dict] = []
    new_deps: list[LegacyDependency] = []
    removed_deps: list[LegacyDependency] = []

    # and now, we can do that diff
    diff = Diff(config, cache)
    for i, pkg in enumerate(transformer.crates.values()):
        pkg_id, pkg_obj, update_payload = diff.diff_pkg(pkg)
        if pkg_obj:
            new_packages.append(pkg_obj)
        if update_payload:
            updated_packages.append(update_payload)

        # URLs
        resolved_urls = diff.diff_url(pkg, new_urls)

        # package URLs
        new_links, updated_links = diff.diff_pkg_url(pkg_id, resolved_urls)
        if new_links:
            new_package_urls.extend(new_links)
        if updated_links:
            updated_package_urls.extend(updated_links)

        # finally, dependencies
        new_dependencies, removed_dependencies = diff.diff_deps(pkg)
        if new_dependencies:
            new_deps.extend(new_dependencies)
        if removed_dependencies:
            removed_deps.extend(removed_dependencies)

    db.ingest(
        new_packages,
        new_urls,
        new_package_urls,
        new_deps,
        removed_deps,
        updated_packages,
        updated_package_urls,
    )

    logger.log("✅ Done")


if __name__ == "__main__":
    config = Config(PackageManager.CRATES)
    db = CratesDB(config)
    db.set_current_graph()
    main(config, db)
