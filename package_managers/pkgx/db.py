#!/usr/bin/env pkgx uv run

from datetime import datetime
from uuid import UUID

from sqlalchemy import Result, select, update

from core.config import Config
from core.db import DB, CurrentURLs
from core.models import URL, LegacyDependency, Package, PackageURL
from core.structs import CurrentGraph, URLKey


class PkgxDB(DB):
    def __init__(self, logger_name: str, config: Config):
        super().__init__(logger_name)
        self.config = config

    def set_current_graph(self) -> None:
        """Get the pkgx packages and dependencies"""
        self.graph: CurrentGraph = self.current_graph(self.config.pm_config.pm_id)
        self.logger.log(f"Loaded {len(self.graph.package_map)} pkgx packages")

    def set_current_urls(self) -> None:
        """Getting all the URLs and Package URLs from the database"""
        self.urls: CurrentURLs | None = None
        url_map: dict[URLKey, URL] = {}
        package_urls: dict[UUID, set[PackageURL]] = {}

        stmt = (
            select(Package, PackageURL, URL)
            .select_from(URL)
            .join(PackageURL, PackageURL.url_id == URL.id, isouter=True)
            .join(Package, Package.id == PackageURL.package_id, isouter=True)
        )
        with self.session() as session:
            result: Result[tuple[Package, PackageURL, URL]] = session.execute(stmt)

            for pkg, pkg_url, url in result:
                url_key = URLKey(url.url, url.url_type_id)
                url_map[url_key] = url

                # since it's a left join, we need to check if pkg is None
                if pkg is not None:
                    if pkg.id not in package_urls:
                        package_urls[pkg.id] = set()
                    package_urls[pkg.id].add(pkg_url)

        self.urls = CurrentURLs(url_map=url_map, package_urls=package_urls)

    def ingest(
        self,
        new_packages: list[Package],
        new_urls: list[URL],
        new_package_urls: list[PackageURL],
        updated_packages: list[dict[str, UUID | str | datetime]],
        updated_package_urls: list[dict[str, UUID | datetime]],
        new_deps: list[LegacyDependency],
        removed_deps: list[LegacyDependency],
    ) -> None:
        """
        Ingest the diffs by first adding all new entities, then updating existing ones.

        Inputs:
          - All the differential changes computed by the diff module

        Outputs:
          - None
        """
        self.logger.log("-" * 100)
        self.logger.log("Going to load pkgx data")
        self.logger.log(f"New packages: {len(new_packages)}")
        self.logger.log(f"New URLs: {len(new_urls)}")
        self.logger.log(f"New package URLs: {len(new_package_urls)}")
        self.logger.log(f"Updated packages: {len(updated_packages)}")
        self.logger.log(f"Updated package URLs: {len(updated_package_urls)}")
        self.logger.log(f"New dependencies: {len(new_deps)}")
        self.logger.log(f"Removed dependencies: {len(removed_deps)}")
        self.logger.log("-" * 100)

        with self.session() as session:
            try:
                # 1. Add all new objects with granular flushes
                if new_packages:
                    session.add_all(new_packages)
                    session.flush()

                if new_urls:
                    session.add_all(new_urls)
                    session.flush()

                if new_package_urls:
                    session.add_all(new_package_urls)
                    session.flush()

                # remove deps first to avoid constraint issues
                if removed_deps:
                    for dep in removed_deps:
                        session.delete(dep)
                    session.flush()

                if new_deps:
                    session.add_all(new_deps)
                    session.flush()

                # 2. Perform updates (these will now operate on a flushed state)
                if updated_packages:
                    session.execute(update(Package), updated_packages)

                if updated_package_urls:
                    session.execute(update(PackageURL), updated_package_urls)

                # 3. Commit all changes
                session.commit()
                self.logger.log("✅ Successfully ingested pkgx data")

            except Exception as e:
                self.logger.error(f"Error during pkgx batched ingest: {e}")
                session.rollback()
                raise e
