from datetime import datetime
from uuid import UUID, uuid4

from permalint import is_canonical_url, normalize_url

from core.db import DB
from core.logger import Logger
from core.models import URL, Canon, CanonPackage, Package, PackageURL
from core.utils import env_vars
from ranker.config import Config, load_config

# TODO: add both of these to the config object for the ranker
LOAD = env_vars("LOAD", "true")
TEST = env_vars("TEST", "false")


class DedupeDB(DB):
    def __init__(self, config: Config):
        super().__init__("ranker.db")
        self.config: Config = config

    def get_current_canons(self) -> dict[UUID, Canon]:
        """Get current canons as a mapping from URL to Canon object."""
        with self.session() as session:
            canons = session.query(Canon).all()
            return {canon.url_id: canon for canon in canons}

    def get_current_canon_packages(self) -> dict[UUID, UUID]:
        """Get current canon-package mappings as dict[package_id -> canon_id]."""
        with self.session() as session:
            canon_packages = session.query(CanonPackage).all()
            return {cp.package_id: cp.canon_id for cp in canon_packages}

    def get_packages_with_homepages(self) -> list[tuple[Package, URL]]:
        with self.session() as session:
            return (
                session.query(Package, URL)
                .join(PackageURL, Package.id == PackageURL.package_id)
                .join(URL, PackageURL.url_id == URL.id)
                .where(URL.url_type_id == self.config.url_types.homepage_url_type_id)
                .order_by(
                    Package.id, URL.created_at.desc()
                )  # Latest URL per package first
                .all()
            )

    def update_canon_url(self, canon_id: UUID, new_url: str) -> None:
        """Update an existing canon's URL."""
        with self.session() as session:
            canon = session.query(Canon).filter(Canon.id == canon_id).first()
            if canon:
                canon.url = new_url
                session.commit()

    def create_canon(self, canon: Canon) -> None:
        """Create a new canon."""
        with self.session() as session:
            session.add(canon)
            session.commit()

    def update_canon_package_mapping(self, package_id: UUID, canon_id: UUID) -> None:
        """Update or create canon-package mapping."""
        with self.session() as session:
            # Check if mapping exists
            existing = (
                session.query(CanonPackage)
                .filter(CanonPackage.package_id == package_id)
                .first()
            )

            if existing:
                existing.canon_id = canon_id
            else:
                from uuid import uuid4

                new_mapping = CanonPackage(
                    id=uuid4(), canon_id=canon_id, package_id=package_id
                )
                session.add(new_mapping)

            session.commit()


def get_latest_homepage_per_package(
    packages_with_homepages: list[tuple[Package, URL]],
) -> dict[UUID, URL]:
    """Get the latest homepage URL for each package."""
    latest_homepages: dict[UUID, URL] = {}

    for pkg, url in packages_with_homepages:
        # Since we ordered by Package.id, URL.created_at.desc(),
        # the first URL we see for each package is the latest
        if pkg.id not in latest_homepages:
            latest_homepages[pkg.id] = url

    return latest_homepages


def main(config: Config, db: DedupeDB):
    logger = Logger("ranker.dedupe_v2")
    now = datetime.now()
    logger.debug(f"Starting deduplication process at {now}")

    # 1. Get current state
    current_canons: dict[UUID, Canon] = db.get_current_canons()
    logger.debug(f"Found {len(current_canons)} current canons")

    # check if any canons are not canonicalized, and throw if so
    for url, canon in current_canons.items():
        if not is_canonical_url(url):
            raise Exception(f"{canon.id}: {url} is not canonicalized")

    current_canon_packages: dict[UUID, UUID] = db.get_current_canon_packages()
    logger.debug(f"Found {len(current_canon_packages)} current canon packages")

    packages_with_homepages: list[tuple[Package, URL]] = (
        db.get_packages_with_homepages()
    )
    logger.debug(f"Found {len(packages_with_homepages)} packages with homepages")

    # 2. Get latest homepage per package
    latest_homepages: dict[UUID, URL] = get_latest_homepage_per_package(
        packages_with_homepages
    )
    logger.debug(f"Found {len(latest_homepages)} packages with latest homepages")

    # 3. Process changes differentially
    canons_to_update: list[tuple[UUID, str]] = []  # (canon_id, new_url)
    canons_to_create: list[Canon] = []
    mappings_to_update: list[tuple[UUID, UUID]] = []  # (package_id, canon_id)
    mappings_to_create: list[tuple[UUID, UUID]] = []  # (package_id, canon_id)

    for pkg_id, url in latest_homepages.items():
        # does the url have a canon?
        actual_canon_id = current_canons.get(url.id)

        # is the package tied to a canon?
        linked_canon_id = current_canon_packages.get(pkg_id)

        if actual_canon_id is None:
            # this URL is not associated with a canon, so we need to create one
            # name: TODO?
            new_canon = Canon(
                id=uuid4(),
                url_id=url.id,
                name=url.url,
                created_at=now,
                updated_at=now,
            )
            canons_to_create.append(new_canon)

            # now, is the package tied to a canon?
            if linked_canon_id is None:
                # new canon package!
                new_canon_package = CanonPackage(
                    id=uuid4(),
                    canon_id=new_canon.id,
                    package_id=pkg_id,
                    created_at=now,
                    updated_at=now,
                )
                mappings_to_create.append((pkg_id, new_canon.id))
            else:
                # update the existing canon package row
                mappings_to_update.append((pkg_id, new_canon.id))
        else:
            # ok, this is an existing canon
            # it also cannot exist in the mappings, so it means a new canon package to
            pass

    # 5. Apply changes
    logger.log("-" * 100)
    logger.log("Changes to apply:")
    logger.log(f"  Canons to update: {len(canons_to_update)}")
    logger.log(f"  Canons to create: {len(canons_to_create)}")
    logger.log(f"  Mappings to update: {len(mappings_to_update)}")
    logger.log("-" * 100)

    if not LOAD:
        logger.log("Skipping changes because LOAD is not set")
        return

    # Update existing canon URLs
    for canon_id, new_url in canons_to_update:
        logger.debug(f"Updating canon {canon_id} URL to: {new_url}")
        db.update_canon_url(canon_id, new_url)

    # Create new canons
    for canon in canons_to_create:
        logger.debug(f"Creating new canon {canon.id} for URL: {canon.url}")
        db.create_canon(canon)

    # Update canon-package mappings
    for package_id, canon_id in mappings_to_update:
        logger.debug(f"Updating package {package_id} to canon {canon_id}")
        db.update_canon_package_mapping(package_id, canon_id)

    logger.log("✅ Deduplication process completed")


if __name__ == "__main__":
    config: Config = load_config()
    db: DedupeDB = DedupeDB(config)
    main(config, db)
