from uuid import UUID

from sqlalchemy.dialects.postgresql import insert as pg_insert

from core.db import DB
from core.models import (
    URL,
    Canon,
    CanonPackage,
    DependsOn,
    LegacyDependency,
    Package,
    PackageURL,
    TeaRank,
    TeaRankRun,
    URLType,
    Version,
)

BATCH_SIZE = 20000


class GraphDB(DB):
    def __init__(self, legacy_pm_id: UUID, system_pm_ids: list[UUID]):
        super().__init__("graph.db")
        self.legacy_pm_id = legacy_pm_id
        self.system_pm_ids = system_pm_ids

    def is_canon_populated(self) -> bool:
        with self.session() as session:
            return session.query(Canon).count() > 0

    def is_canon_package_populated(self) -> bool:
        with self.session() as session:
            return session.query(CanonPackage).count() > 0

    def get_all_canons(self) -> dict[str, UUID]:
        """Fetch all existing canons as a map from URL to Canon ID."""
        with self.session() as session:
            results = session.query(Canon.url, Canon.id).all()
            return dict(results)

    def get_packages_with_urls(self) -> list[tuple[UUID, str, str, str]]:
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
                .where(URLType.name == "homepage")  # we're deduplicating on homepage
                .order_by(URL.created_at.desc())
                .all()
            )

    def load_canonical_packages(self, data: list[Canon]) -> None:
        """
        Load canonical packages into the database in batches, handling conflicts.

        Args:
            data: List of Canon objects.
        """
        with self.session() as session:
            for i in range(0, len(data), BATCH_SIZE):
                batch = data[i : i + BATCH_SIZE]
                if not batch:
                    continue

                # Convert batch objects to dictionaries for insert statement
                insert_data = [
                    {"id": item.id, "url": item.url, "name": item.name}
                    for item in batch
                ]

                stmt = pg_insert(Canon).values(insert_data)
                stmt = stmt.on_conflict_do_nothing(index_elements=["url"])

                if stmt is not None:
                    session.execute(stmt)

                # log
                batch_number = (i // BATCH_SIZE) + 1
                total_batches = (len(data) + BATCH_SIZE - 1) // BATCH_SIZE
                self.logger.log(
                    f"Processed Canon batch {batch_number} of {total_batches}"
                )

            session.commit()

    def load_canonical_package_mappings(self, data: list[CanonPackage]) -> None:
        """
        Load canonical package mappings into the database in batches, updating on
        conflict.

        Args:
            data: List of CanonPackage objects.
        """
        with self.session() as session:
            for i in range(0, len(data), BATCH_SIZE):
                batch = data[i : i + BATCH_SIZE]
                if not batch:
                    continue

                # Convert batch objects to dictionaries
                insert_data = [
                    {
                        "id": item.id,
                        "canon_id": item.canon_id,
                        "package_id": item.package_id,
                    }
                    for item in batch
                ]

                stmt = pg_insert(CanonPackage).values(insert_data)
                update_dict = {"canon_id": stmt.excluded.canon_id}

                # this is the unique constraint on canon_packages -> if its violated,
                # that means that the package has changed its URL, and the dedupe
                # logic has corrected the correct canon for this package
                stmt = stmt.on_conflict_do_update(
                    index_elements=["package_id"], set_=update_dict
                )

                if stmt is not None:
                    session.execute(stmt)

                # log
                batch_number = (i // BATCH_SIZE) + 1
                total_batches = (len(data) + BATCH_SIZE - 1) // BATCH_SIZE
                self.logger.log(
                    f"Processed CanonPackage batch {batch_number} of {total_batches}"
                )

            session.commit()

    def get_packages(self) -> list[tuple[UUID, UUID]]:
        """Gets all packages for the run"""
        self.logger.debug(f"Getting packages for {self.system_pm_ids} package managers")
        with self.session() as session:
            return (
                session.query(Package.id, Package.package_manager_id)
                .where(Package.package_manager_id.in_(self.system_pm_ids))
                .all()
            )

    def get_dependencies(self, package_id: UUID) -> list[tuple[UUID]]:
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

    def get_legacy_dependencies(self, package_id: UUID) -> list[tuple[UUID]]:
        """Gets all the legacy dependencies based on the legacy CHAI data model"""
        with self.session() as session:
            return (
                session.query(LegacyDependency.dependency_id)
                .filter(LegacyDependency.package_id == package_id)
                .filter(LegacyDependency.dependency_id != package_id)
                .all()
            )

    def load_tea_ranks(self, data: list[TeaRank]) -> None:
        """Loads tea ranks into the database"""
        with self.session() as session:
            session.add_all(data)
            session.commit()

    def load_tea_rank_runs(self, data: list[TeaRankRun]) -> None:
        """Loads tea rank runs into the database"""
        with self.session() as session:
            session.add_all(data)
            session.commit()

    def get_current_tea_rank_run(self) -> TeaRankRun | None:
        """Gets the current tea rank run"""
        with self.session() as session:
            return (
                session.query(TeaRankRun).order_by(TeaRankRun.created_at.desc()).first()
            )
