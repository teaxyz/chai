from datetime import datetime
from typing import Dict, List, Set, Tuple, Union
from uuid import UUID

from sqlalchemy import Result, select, update

from core.config import Config
from core.db import DB, CurrentURLs
from core.models import URL, LegacyDependency, Package, PackageURL


class HomebrewDB(DB):
    def __init__(self, logger_name: str, config: Config):
        super().__init__(logger_name)
        self.config = config
        self.current_graph()
        self.logger.log(f"{len(self.package_map)} packages from Homebrew")

    def current_graph(self) -> None:
        """Get the Homebrew packages and dependencies"""
        self.package_map: Dict[str, Package] = {}  # name to package
        self.dependencies: Dict[UUID, Set[LegacyDependency]] = {}

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
            result: Result[Tuple[Package, LegacyDependency]] = session.execute(stmt)

            for pkg, dep in result:
                # add to the package map
                if pkg.name not in self.package_map:
                    self.package_map[pkg.name] = pkg

                # and add to the dependencies map as well
                if dep:  # check because it's an outer join
                    if pkg.id not in self.dependencies:
                        self.dependencies[pkg.id] = set()
                    self.dependencies[pkg.id].add(dep)

    def set_current_urls(self, urls: Set[str]) -> None:
        """Wrapper for setting current urls"""
        self.current_urls: CurrentURLs = self.get_current_urls(urls)
        self.logger.debug(f"Found {len(self.current_urls.url_map)} Homebrew URLs")

    def ingest(
        self,
        new_packages: List[Package],
        new_urls: List[URL],
        new_package_urls: List[PackageURL],
        updated_packages: List[Dict[str, Union[UUID, str, datetime]]],
        updated_package_urls: List[Dict[str, Union[UUID, datetime]]],
        new_deps: List[LegacyDependency],
        removed_deps: List[LegacyDependency],
    ) -> None:
        """
        Ingest the diffs by first adding all new entities, then updating existing ones.

        Inputs:
          - diffs: a list of Diff objects

        Outputs:
          - None
        """
        self.logger.log("-" * 100)
        self.logger.log("Going to load")
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

                # we should remove deps first
                if removed_deps:
                    for dep in removed_deps:  # helper doesn't exist?
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
                self.logger.log("✅ Successfully ingested")
            except Exception as e:
                self.logger.error(f"Error during batched ingest: {e}")
                session.rollback()
                raise e
