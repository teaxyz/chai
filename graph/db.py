from typing import List, Tuple
from uuid import UUID

from core.db import DB
from core.models import (
    URL,
    Canon,
    CanonPackage,
    Package,
    PackageURL,
    URLType,
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
