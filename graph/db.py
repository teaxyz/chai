from typing import List, Tuple
from uuid import UUID

from sqlalchemy import func

from core.db import DB
from core.models import (
    URL,
    Canon,
    CanonPackage,
    DependsOn,
    LegacyDependency,
    Package,
    PackageManager,
    PackageURL,
    Source,
    URLType,
    Version,
)

BATCH_SIZE = 20000


class GraphDB(DB):
    def __init__(self):
        super().__init__("graph_db")

    def is_canon_populated(self) -> bool:
        with self.session() as session:
            return session.query(Canon).count() > 0

    def is_canon_package_populated(self) -> bool:
        with self.session() as session:
            return session.query(CanonPackage).count() > 0

    def get_packages_with_urls(self) -> List[Tuple[UUID, UUID, str, UUID, str, str]]:
        """
        Retrieve packages with their associated URLs and URL types.

        Returns:
            List of tuples containing package details and their URLs.
        """
        with self.session() as session:
            return (
                session.query(
                    Package.id,
                    Package.name,
                    URL.url,
                )
                .join(PackageURL, Package.id == PackageURL.package_id)
                .join(URL, PackageURL.url_id == URL.id)
                .join(URLType, URL.url_type_id == URLType.id)
                .where(URLType.name == "homepage")  # TODO: is this assumpion okay?
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

    def get_canons(self) -> List[Canon]:
        with self.session() as session:
            return session.query(Canon.id).all()

    def get_package_dependencies(self, canon_id: UUID) -> List[UUID]:
        with self.session() as session:
            return (
                session.query(DependsOn.dependency_id)
                .join(Version, DependsOn.version_id == Version.id)
                .join(Package, Version.package_id == Package.id)
                .join(CanonPackage, Package.id == CanonPackage.package_id)
                .filter(CanonPackage.canon_id == canon_id)
                .all()
            )

    def get_canon_packages(self) -> dict[UUID, UUID]:
        with self.session() as session:
            return {
                canon_package.package_id: canon.id
                for canon, canon_package in session.query(Canon, CanonPackage).join(
                    CanonPackage, Canon.id == CanonPackage.canon_id
                )
            }

    def get_canons_by_package_manager(self, package_manager: str) -> List[UUID]:
        with self.session() as session:
            return (
                session.query(Canon.id)
                .join(CanonPackage, Canon.id == CanonPackage.canon_id)
                .join(Package, CanonPackage.package_id == Package.id)
                .join(PackageManager, Package.package_manager_id == PackageManager.id)
                .join(Source, PackageManager.source_id == Source.id)
                .filter(Source.type == package_manager)
                .all()
            )

    def get_canons_with_source_types(
        self, source_types: List[str]
    ) -> List[Tuple[UUID, List[str]]]:
        """
        Get canons and their associated source types (package managers).

        Args:
            source_types: List of source types to filter by (e.g., ['homebrew', 'debian'])

        Returns:
            List of tuples containing canon IDs and lists of their associated source types
        """

        with self.session() as session:
            return (
                session.query(
                    Canon.id, func.array_agg(Source.type).label("source_types")
                )
                .join(CanonPackage, Canon.id == CanonPackage.canon_id)
                .join(Package, CanonPackage.package_id == Package.id)
                .join(PackageManager, Package.package_manager_id == PackageManager.id)
                .join(Source, PackageManager.source_id == Source.id)
                .filter(Source.type.in_(source_types))
                .group_by(Canon.id)
                .all()
            )

    def get_npm_dependencies(self, package_id: UUID) -> List[UUID]:
        with self.session() as session:
            return (
                session.query(LegacyDependency.dependency_id)
                .filter(LegacyDependency.package_id == package_id)
                .all()
            )
