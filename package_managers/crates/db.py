from uuid import UUID

from sqlalchemy import select

from core.config import Config
from core.db import DB
from core.models import (
    CanonPackage,
    DependsOn,
    LegacyDependency,
    Package,
    PackageURL,
    UserPackage,
    UserVersion,
    Version,
)
from core.structs import CurrentGraph, CurrentURLs


class CratesDB(DB):
    def __init__(self, config: Config):
        super().__init__("crates_db")
        self.config = config
        # self.set_current_graph()

    def set_current_graph(self) -> None:
        self.graph: CurrentGraph = self.current_graph(self.config.pm_config.pm_id)

    def set_current_urls(self, urls: set[str]) -> None:
        self.urls: CurrentURLs = self.current_urls(urls)

    def delete_packages_by_import_id(self, import_ids: set[int]) -> None:
        """
        Delete packages identified by import_ids and all their dependent records.
        This is a DB class method to handle the cascade deletion properly.
        """

        # Convert import_ids to package_ids using the cache
        package_ids: list[UUID] = []
        for import_id in import_ids:
            package = self.graph.package_map.get(str(import_id))
            if package:
                package_ids.append(package.id)

        if not package_ids:
            self.logger.debug("No packages found to delete")
            return

        self.logger.debug(f"Deleting {len(package_ids)} crates completely")

        # Delete records in reverse dependency order
        with self.session() as session:
            try:
                # 1. Delete PackageURLs
                package_urls_deleted = (
                    session.query(PackageURL)
                    .filter(PackageURL.package_id.in_(package_ids))
                    .delete(synchronize_session=False)
                )

                # 2. Delete CanonPackages
                canon_packages_deleted = (
                    session.query(CanonPackage)
                    .filter(CanonPackage.package_id.in_(package_ids))
                    .delete(synchronize_session=False)
                )

                # 3. Delete UserPackages
                user_packages_deleted = (
                    session.query(UserPackage)
                    .filter(UserPackage.package_id.in_(package_ids))
                    .delete(synchronize_session=False)
                )

                # 4. Delete LegacyDependencies (both package_id and dependency_id)
                legacy_deps_package_deleted = (
                    session.query(LegacyDependency)
                    .filter(LegacyDependency.package_id.in_(package_ids))
                    .delete(synchronize_session=False)
                )

                legacy_deps_dependency_deleted = (
                    session.query(LegacyDependency)
                    .filter(LegacyDependency.dependency_id.in_(package_ids))
                    .delete(synchronize_session=False)
                )

                # TODO: this table is deprecated, but still contains records
                # we can remove this line, once all indexers use LegacyDependencies
                # 5. Delete DependsOn where dependency_id is in package_ids
                depends_on_deleted = (
                    session.query(DependsOn)
                    .filter(DependsOn.dependency_id.in_(package_ids))
                    .delete(synchronize_session=False)
                )

                # 6. Delete Versions and their dependencies
                # TODO: remove this line once all indexers stop using Versions and
                # we can truncate this table
                # First get all version ids for these packages
                version_ids = [
                    vid
                    for (vid,) in session.query(Version.id).filter(
                        Version.package_id.in_(package_ids)
                    )
                ]

                # Delete dependencies attached to these versions
                version_deps_deleted = 0
                user_versions_deleted = 0
                if version_ids:
                    version_deps_deleted = (
                        session.query(DependsOn)
                        .filter(DependsOn.version_id.in_(version_ids))
                        .delete(synchronize_session=False)
                    )

                    user_versions_deleted = (
                        session.query(UserVersion)
                        .filter(UserVersion.version_id.in_(version_ids))
                        .delete(synchronize_session=False)
                    )

                # Now delete the versions
                versions_deleted = (
                    session.query(Version)
                    .filter(Version.package_id.in_(package_ids))
                    .delete(synchronize_session=False)
                )

                # 7. Finally delete the packages
                packages_deleted = (
                    session.query(Package)
                    .filter(Package.id.in_(package_ids))
                    .delete(synchronize_session=False)
                )

                self.logger.debug("-" * 100)
                self.logger.debug("Going to commit delete for")
                self.logger.debug(f"{packages_deleted} packages")
                self.logger.debug(f"{versions_deleted} versions")
                self.logger.debug(f"{version_deps_deleted} version dependencies")
                self.logger.debug(f"{user_versions_deleted} user versions")
                self.logger.debug(f"{depends_on_deleted} direct dependencies")
                self.logger.debug(
                    f"{legacy_deps_package_deleted + legacy_deps_dependency_deleted} legacy deps"  # noqa E501
                )
                self.logger.debug(f"{user_packages_deleted} user packages")
                self.logger.debug(f"{canon_packages_deleted} canon packages")
                self.logger.debug(f"{package_urls_deleted} package URLs")
                self.logger.debug("-" * 100)

                # Commit the transaction
                session.commit()

            except Exception as e:
                session.rollback()
                self.logger.error(f"Error deleting packages: {e}")
                raise

    def get_cargo_id_to_chai_id(self) -> dict[str, UUID]:
        """
        Returns a map of cargo import_ids to chai_ids
        """
        with self.session() as session:
            stmt = select(Package.import_id, Package.id).where(
                Package.package_manager_id == self.config.pm_config.pm_id
            )
            return {row[0]: row[1] for row in session.execute(stmt).all()}
