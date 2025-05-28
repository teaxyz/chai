from uuid import UUID

from permalint import normalize_url

from core.db import DB
from core.logger import Logger
from core.models import URL, Canon, CanonPackage, Package, PackageURL
from ranker.config import Config, load_config


class DedupeDB(DB):
    def __init__(self, config: Config):
        super().__init__("ranker.db")
        self.config: Config = config

    def get_current_canons(self) -> dict[str, Canon]:
        """Get current canons as a mapping from URL to Canon object."""
        with self.session() as session:
            canons = session.query(Canon).all()
            return {canon.url: canon for canon in canons}

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


def find_canon_for_url(url: str, current_canons: dict[str, Canon]) -> Canon | None:
    """Find an existing canon that should be used for this URL.

    This handles cases where:
    1. Exact URL match exists
    2. Canonicalized version of existing canon URL matches this URL
    """
    # Direct match
    if url in current_canons:
        return current_canons[url]

    # Check if this URL is a canonicalized version of any existing canon
    for existing_url, canon in current_canons.items():
        if normalize_url(existing_url) == url:
            return canon

    return None


def main(config: Config, db: DedupeDB):
    logger = Logger("ranker.dedupe_v2")
    logger.debug("Starting improved deduplication process")

    # 1. Get current state
    current_canons: dict[str, Canon] = db.get_current_canons()
    logger.debug(f"Found {len(current_canons)} current canons")

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

    # 3. Validate all URLs are permalinted
    not_permalinted: set[URL] = set()
    for pkg_id, url in latest_homepages.items():
        try:
            canonicalized = normalize_url(url.url)
            if canonicalized != url.url:
                not_permalinted.append(url)
        except Exception as e:
            logger.warn(f"Potential bug for `permalint` on {url.url}: {e}")
            not_permalinted.append(url)

    if not_permalinted:
        logger.warn(f"Found {len(not_permalinted)} not permalinted homepages")
        logger.warn(
            "The following homepages are not permalinted, please fix them manually:"
        )
        raise Exception("Not permalinted homepages found")

    # 4. Process changes differentially
    canons_to_update: list[tuple[UUID, str]] = []  # (canon_id, new_url)
    canons_to_create: list[Canon] = []
    mappings_to_update: list[tuple[UUID, UUID]] = []  # (package_id, canon_id)

    for pkg_id, url in latest_homepages.items():
        current_canon_id = current_canon_packages.get(pkg_id)
        target_canon = find_canon_for_url(url.url, current_canons)

        if current_canon_id is None:
            # Package has no canon mapping yet
            if target_canon:
                # Link to existing canon
                mappings_to_update.append((pkg_id, target_canon.id))
            else:
                # Create new canon
                from uuid import uuid4

                new_canon_id = uuid4()
                # Get package name for canon name (you might want to improve this logic)
                pkg_name = next(
                    pkg.name for pkg, _ in packages_with_homepages if pkg.id == pkg_id
                )
                new_canon = Canon(id=new_canon_id, name=pkg_name, url=url.url)
                canons_to_create.append(new_canon)
                mappings_to_update.append((pkg_id, new_canon_id))
        else:
            # Package already has a canon mapping
            current_canon = next(
                c for c in current_canons.values() if c.id == current_canon_id
            )

            if target_canon and target_canon.id != current_canon_id:
                # URL matches a different existing canon, update mapping
                mappings_to_update.append((pkg_id, target_canon.id))
            elif current_canon.url != url.url:
                # Same canon but URL has changed, update the canon's URL
                canons_to_update.append((current_canon_id, url.url))

    # 5. Apply changes
    logger.debug(f"Changes to apply:")
    logger.debug(f"  Canons to update: {len(canons_to_update)}")
    logger.debug(f"  Canons to create: {len(canons_to_create)}")
    logger.debug(f"  Mappings to update: {len(mappings_to_update)}")

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

    logger.log("✅ Improved deduplication process completed")


if __name__ == "__main__":
    config: Config = load_config()
    db: DedupeDB = DedupeDB(config)
    main(config, db)
