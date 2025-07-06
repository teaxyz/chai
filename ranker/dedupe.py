#!/usr/bin/env uv run --with sqlalchemy==2.0.34 --with permalint==0.1.12
from datetime import datetime
from uuid import UUID, uuid4

from permalint import is_canonical_url
from sqlalchemy import update
from sqlalchemy.orm import Session

from core.db import DB
from core.logger import Logger
from core.models import URL, BaseModel, Canon, CanonPackage, Package, PackageURL
from package_managers.crates.structs import (
    CanonPackageUpdatePayload,
    CanonUpdatePayload,
)
from ranker.config import DedupeConfig, load_dedupe_config
from ranker.naming import compute_canon_name, get_effective_canon_name


class DedupeDB(DB):
    def __init__(self, config: DedupeConfig):
        super().__init__("ranker.db")
        self.config: DedupeConfig = config

    def get_current_canons(self) -> dict[UUID, Canon]:
        """Get current canons as a mapping from URL ID to Canon object."""
        with self.session() as session:
            canons = session.query(Canon).all()
            return {canon.url_id: canon for canon in canons}

    def get_current_canon_packages(self) -> dict[UUID, dict[str, UUID]]:
        """Get current canon-package mappings as dict[package_id -> canon_id]."""
        with self.session() as session:
            canon_packages = session.query(CanonPackage).all()
            return {
                cp.package_id: {"id": cp.id, "canon_id": cp.canon_id}
                for cp in canon_packages
            }

    def get_packages_with_homepages(self) -> list[tuple[Package, URL]]:
        with self.session() as session:
            return (
                session.query(Package, URL)
                .join(PackageURL, Package.id == PackageURL.package_id)
                .join(URL, PackageURL.url_id == URL.id)
                .where(URL.url_type_id == self.config.homepage_url_type_id)
                .order_by(Package.id, URL.created_at.desc())  # Latest URL / package
                .all()
            )

    def get_all_package_names(self) -> dict[UUID, str]:
        with self.session() as session:
            return {pkg.id: pkg.name for pkg in session.query(Package).all()}

    # TODO: first to be optimized
    def ingest(
        self,
        new_canons: list[Canon],
        canon_updates: list[CanonUpdatePayload],
        new_canon_packages: list[CanonPackage],
        updated_canon_packages: list[CanonPackageUpdatePayload],
    ) -> None:
        with self.session() as session:
            if new_canons:
                self.add_with_flush(session, new_canons)

            if canon_updates:
                session.execute(update(Canon), canon_updates)

            if new_canon_packages:
                self.add_with_flush(session, new_canon_packages)

            if updated_canon_packages:
                session.execute(update(CanonPackage), updated_canon_packages)

            session.commit()

    def add_with_flush(self, session: Session, rows: list[BaseModel]) -> None:
        session.add_all(rows)
        session.flush()


def get_latest_homepage_per_package(
    packages_with_homepages: list[tuple[Package, URL]], logger: Logger
) -> tuple[dict[UUID, URL], list[URL]]:
    """Get the latest homepage URL for each package."""
    latest_homepages: dict[UUID, URL] = {}
    non_canonical_urls: list[URL] = []

    for pkg, url in packages_with_homepages:
        # Since we ordered by Package.id, URL.created_at.desc(),
        # the first URL we see for each package is the latest
        if pkg.id not in latest_homepages:
            # skip empty or whitespace-only urls
            if not url.url or url.url.strip() == "":
                continue

            # guard against non-canonicalized URLs
            try:
                if not is_canonical_url(url.url):
                    non_canonical_urls.append(url)
                else:
                    latest_homepages[pkg.id] = url
            except Exception as e:
                logger.warn(f"Error checking if {url.url} is canonical: {e}")
                non_canonical_urls.append(url)

    if non_canonical_urls:
        logger.warn(f"Found {len(non_canonical_urls)} non-canonicalized URLs in URLs")

    return latest_homepages, non_canonical_urls


def build_canon_update_payload(
    canon: Canon, new_name: str, now: datetime
) -> CanonUpdatePayload:
    """Build update payload for a canon with a new name."""
    return CanonUpdatePayload(id=canon.id, name=new_name, updated_at=now)


def build_canon_package_update_payload(
    current_canon_packages: dict[UUID, dict[str, UUID]],
    pkg_id: UUID,
    new_canon_id: UUID,
    now: datetime,
) -> CanonPackageUpdatePayload:
    """Build an update payload for a canon package."""
    canon_package_data = current_canon_packages.get(pkg_id)

    if canon_package_data is None:
        raise ValueError(f"No canon package mappings for {pkg_id}")

    current_canon_package_id = canon_package_data.get("id")
    if current_canon_package_id is None:
        raise ValueError(f"{pkg_id} has no canon package ID but canon: {new_canon_id}")

    return CanonPackageUpdatePayload(
        id=current_canon_package_id, canon_id=new_canon_id, updated_at=now
    )


def process_deduplication_changes(
    latest_homepages: dict[UUID, URL],
    current_canons: dict[UUID, Canon],
    current_canon_packages: dict[UUID, dict[str, UUID]],
    name_map: dict[UUID, str],
    logger: Logger,
) -> tuple[
    list[Canon],
    list[CanonUpdatePayload],
    list[CanonPackage],
    list[CanonPackageUpdatePayload],
]:
    """
    Process deduplication changes based on current state.

    Returns:
        tuple of (canons_to_create, canons_to_update, mappings_to_create,
        mappings_to_update)
    """
    now = datetime.now()
    canons_to_create: dict[UUID, Canon] = {}  # indexed by url_id for deduplication
    canons_to_update: dict[UUID, CanonUpdatePayload] = {}  # indexed by canon_id
    mappings_to_create: list[CanonPackage] = []
    mappings_to_update: list[CanonPackageUpdatePayload] = []

    for pkg_id, url in latest_homepages.items():
        # Check if the URL has an existing canon
        existing_canon: Canon | None = current_canons.get(url.id)

        # If no existing canon, check if we're creating one for this URL
        if existing_canon is None:
            existing_canon = canons_to_create.get(url.id)

        existing_canon_id: UUID | None = existing_canon.id if existing_canon else None

        # Check if the package is already linked to a canon
        linked_canon_id: UUID | None = current_canon_packages.get(pkg_id, {}).get(
            "canon_id"
        )

        if existing_canon_id is None:
            # No canon exists for this URL - create a new one
            pkg_name = name_map.get(pkg_id)
            # Compute the name for the new canon
            name = compute_canon_name(url.url, pkg_name)
            new_canon = Canon(
                id=uuid4(),
                url_id=url.id,
                name=name,
                created_at=now,
                updated_at=now,
            )
            canons_to_create[url.id] = new_canon

            # Handle package-to-canon mapping
            if linked_canon_id is None:
                # Create new canon package mapping
                new_canon_package = CanonPackage(
                    id=uuid4(),
                    canon_id=new_canon.id,
                    package_id=pkg_id,
                    created_at=now,
                    updated_at=now,
                )
                mappings_to_create.append(new_canon_package)
            else:
                # Update existing mapping to point to new canon
                update_payload = build_canon_package_update_payload(
                    current_canon_packages, pkg_id, new_canon.id, now
                )
                mappings_to_update.append(update_payload)
        else:
            # Canon exists - check if name needs updating

            # Get the most current name (considering pending updates)
            current_name = get_effective_canon_name(existing_canon, canons_to_update)
            pkg_name = name_map.get(pkg_id)
            desired_name = compute_canon_name(url.url, pkg_name, current_name)

            # Update canon name if it's different
            if desired_name != current_name:
                update_payload = build_canon_update_payload(
                    existing_canon, desired_name, now
                )
                canons_to_update[existing_canon.id] = update_payload

            # Handle package-to-canon mapping
            if linked_canon_id is None:
                # Create new canon package mapping
                new_canon_package = CanonPackage(
                    id=uuid4(),
                    canon_id=existing_canon_id,
                    package_id=pkg_id,
                    created_at=now,
                    updated_at=now,
                )
                mappings_to_create.append(new_canon_package)
            elif linked_canon_id != existing_canon_id:
                # Update existing mapping to correct canon
                update_payload = build_canon_package_update_payload(
                    current_canon_packages, pkg_id, existing_canon_id, now
                )
                mappings_to_update.append(update_payload)
            # else: mapping is already correct, no action needed

    return (
        list(canons_to_create.values()),
        list(canons_to_update.values()),
        mappings_to_create,
        mappings_to_update,
    )


def main(config: DedupeConfig, db: DedupeDB):
    logger = Logger("ranker.dedupe")
    now = datetime.now()
    logger.log(f"Starting deduplication process at {now}")

    # 1. Get current state
    current_canons: dict[UUID, Canon] = db.get_current_canons()
    logger.debug(f"Found {len(current_canons)} current canons")

    current_canon_packages: dict[UUID, dict[str, UUID]] = (
        db.get_current_canon_packages()
    )
    logger.debug(f"Found {len(current_canon_packages)} current canon packages")

    packages_with_homepages: list[tuple[Package, URL]] = (
        db.get_packages_with_homepages()
    )
    logger.debug(f"Found {len(packages_with_homepages)} packages with homepages")

    name_map: dict[UUID, str] = db.get_all_package_names()

    # 2. Get latest homepage per package
    latest_homepages, non_canonical_urls = get_latest_homepage_per_package(
        packages_with_homepages, logger
    )
    logger.debug(f"Found {len(latest_homepages)} packages with latest homepages")

    # 3. Process changes differentially
    (canons_to_create, canons_to_update, mappings_to_create, mappings_to_update) = (
        process_deduplication_changes(
            latest_homepages, current_canons, current_canon_packages, name_map, logger
        )
    )

    # 4. Apply changes
    logger.log("-" * 100)
    logger.log("Changes to apply:")
    logger.log(f"  Canons to create: {len(canons_to_create)}")
    logger.log(f"  Canons to update: {len(canons_to_update)}")
    logger.log(f"  Mappings to create: {len(mappings_to_create)}")
    logger.log(f"  Mappings to update: {len(mappings_to_update)}")
    logger.log("-" * 100)

    if not config.load:
        logger.log("Skipping changes because LOAD is not set")
        return

    db.ingest(
        canons_to_create, canons_to_update, mappings_to_create, mappings_to_update
    )

    logger.log("✅ Deduplication process completed")

    if non_canonical_urls:
        logger.warn(f"Found {len(non_canonical_urls)} non-canonical URLs")


if __name__ == "__main__":
    config: DedupeConfig = load_dedupe_config()
    db: DedupeDB = DedupeDB(config)

    try:
        main(config, db)
    finally:
        db.close()
