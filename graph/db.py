from typing import List, Tuple
from uuid import UUID

from core.db import DB
from core.models import (
    URL,
    Canon,
    CanonPackage,
    DependsOn,
    LegacyDependency,
    Package,
    PackageURL,
    URLType,
    Version,
)

BATCH_SIZE = 20000


class GraphDB(DB):
    def __init__(self, legacy_pm_id: UUID, system_pm_ids: List[UUID]):
        super().__init__("graph.db")
        self.legacy_pm_id = legacy_pm_id
        self.system_pm_ids = system_pm_ids

    def is_canon_populated(self) -> bool:
        with self.session() as session:
            return session.query(Canon).count() > 0

    def is_canon_package_populated(self) -> bool:
        with self.session() as session:
            return session.query(CanonPackage).count() > 0

    def get_packages_with_urls(self) -> List[Tuple[UUID, str, str, str]]:
        """
        Retrieve packages with their associated URLs and URL types.

        Returns:
            List of tuples containing id, name, and url
        """
        with self.session() as session:
            return (
                session.query(Package.id, Package.name, URL.url, URL.created_at)
                .join(PackageURL, Package.id == PackageURL.package_id)
                .join(URL, PackageURL.url_id == URL.id)
                .join(URLType, URL.url_type_id == URLType.id)
                .where(URLType.name == "homepage")  # TODO: is this assumpion okay?
                .order_by(URL.created_at.desc())
                .all()
            )

    def load_canonical_packages(self, data: List[Canon]) -> None:
        """
        Load canonical packages into the database in batches

        Args:
            data: List of Canon objects.
        """
        with self.session() as session:
            for i in range(0, len(data), BATCH_SIZE):
                batch = data[i : i + BATCH_SIZE]
                session.add_all(batch)
                session.flush()

                # log
                batch_number = (i // BATCH_SIZE) + 1
                total_batches = (len(data) + BATCH_SIZE - 1) // BATCH_SIZE
                self.logger.log(f"Processed batch {batch_number} of {total_batches}")

            session.commit()

    def load_canonical_package_mappings(self, data: List[CanonPackage]) -> None:
        """
        Load canonical package mappings into the database in batches, returning the ids
        of the canonical package mappings.

        Args:
            data: List of CanonPackage objects.
        """
        with self.session() as session:
            for i in range(0, len(data), BATCH_SIZE):
                batch = data[i : i + BATCH_SIZE]
                session.add_all(batch)
                session.flush()

                # log
                batch_number = (i // BATCH_SIZE) + 1
                total_batches = (len(data) + BATCH_SIZE - 1) // BATCH_SIZE
                self.logger.log(f"Processed batch {batch_number} of {total_batches}")

            session.commit()

    def get_packages(self) -> List[Tuple[UUID, UUID]]:
        """Gets all packages for the run"""
        self.logger.log(f"Getting packages for {self.system_pm_ids} package managers")
        with self.session() as session:
            return (
                session.query(Package.id, Package.package_manager_id)
                .where(Package.package_manager_id.in_(self.system_pm_ids))
                # TODO: remove this where condition for prod runs
                .all()
            )

    def get_dependencies(self, package_id: UUID) -> List[Tuple[UUID]]:
        """Gets all the dependencies based on the CHAI data model"""
        with self.session() as session:
            return (
                session.query(DependsOn.dependency_id)
                .join(Version, DependsOn.version_id == Version.id)
                .join(Package, Version.package_id == Package.id)
                .filter(Package.id == package_id)
                .all()
            )

    def get_package_to_canon_mapping(self) -> dict[UUID, UUID]:
        with self.session() as session:
            return {
                canon_package.package_id: canon.id
                for canon, canon_package in session.query(Canon, CanonPackage)
                .join(CanonPackage, Canon.id == CanonPackage.canon_id)
                .join(Package, CanonPackage.package_id == Package.id)
                .where(Package.package_manager_id != self.legacy_pm_id)
            }

    def get_legacy_dependencies(self, package_id: UUID) -> List[Tuple[UUID]]:
        """Gets all the legacy dependencies based on the legacy CHAI data model"""
        with self.session() as session:
            return (
                session.query(LegacyDependency.dependency_id)
                .filter(LegacyDependency.package_id == package_id)
                .filter(LegacyDependency.dependency_id != package_id)
                .all()
            )
