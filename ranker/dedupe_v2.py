import argparse
import cProfile
import pstats
from datetime import datetime
from uuid import UUID, uuid4

from permalint import is_canonical_url
from sqlalchemy import update
from sqlalchemy.orm import Session

from core.db import DB
from core.logger import Logger
from core.models import URL, BaseModel, Canon, CanonPackage, Package, PackageURL
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
                .where(URL.url_type_id == self.config.url_types.homepage_url_type_id)
                .order_by(Package.id, URL.created_at.desc())  # Latest URL / package
                .all()
            )

    def ingest(
        self,
        new_canons: list[Canon],
        new_canon_packages: list[CanonPackage],
        updated_canon_packages: list[dict[str, UUID | datetime]],
    ) -> None:
        with self.session() as session:
            if new_canons:
                self.add_with_flush(session, new_canons)

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
            # guard against non-canonicalized URLs
            try:
                if not is_canonical_url(url.url):
                    non_canonical_urls.append(url)
            except Exception as e:
                logger.warn(f"Error checking if {url.url} is canonical: {e}")
                non_canonical_urls.append(url)
            else:
                latest_homepages[pkg.id] = url

    if non_canonical_urls:
        logger.warn(f"Found {len(non_canonical_urls)} non-canonicalized URLs in URLs")

    return latest_homepages, non_canonical_urls


def build_update_payload(
    current_canon_packages: dict[UUID, dict[str, UUID]],
    pkg_id: UUID,
    new_canon_id: UUID,
    now: datetime,
    logger: Logger,
) -> dict[str, UUID | datetime] | None:
    """Build an update payload for a canon package."""
    canon_package_data = current_canon_packages.get(pkg_id)
    if canon_package_data is None:
        logger.warn(f"Package {pkg_id} not found in current canon packages")
        return None

    current_canon_package_id = canon_package_data.get("id")
    if current_canon_package_id is None:
        logger.warn(
            f"Package {pkg_id} has no canon package ID but canon: {new_canon_id}"
        )
        return None

    return {"id": current_canon_package_id, "canon_id": new_canon_id, "updated_at": now}


def main(db: DedupeDB):
    logger = Logger("ranker.dedupe_v2")
    now = datetime.now()
    logger.debug(f"Starting deduplication process at {now}")

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

    # 2. Get latest homepage per package
    latest_homepages, non_canonical_urls = get_latest_homepage_per_package(
        packages_with_homepages, logger
    )
    logger.debug(f"Found {len(latest_homepages)} packages with latest homepages")

    # 3. Process changes differentially
    canons_to_create: dict[UUID, Canon] = {}  # indexed by url_id for deduplication
    mappings_to_update: list[tuple[UUID, UUID]] = []  # (id, new_canon_id)
    mappings_to_create: list[CanonPackage] = []

    for pkg_id, url in latest_homepages.items():
        # does the url have a canon?
        actual_canon: Canon | None = current_canons.get(url.id)

        # if not, are we already creating a canon for this URL?
        if actual_canon is None:
            actual_canon = canons_to_create.get(url.id)

        actual_canon_id: UUID | None = actual_canon.id if actual_canon else None

        # is the package tied to a canon?
        linked_canon_id: UUID | None = current_canon_packages.get(pkg_id, {}).get(
            "canon_id"
        )

        if actual_canon_id is None:
            # this URL has no canon and we're not already creating one
            new_canon = Canon(
                id=uuid4(),
                url_id=url.id,
                name=url.url,
                created_at=now,
                updated_at=now,
            )
            canons_to_create[url.id] = new_canon  # Store by URL ID for deduplication

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
                mappings_to_create.append(new_canon_package)
            else:
                update_payload = build_update_payload(
                    current_canon_packages,
                    pkg_id,
                    new_canon.id,
                    now,
                    logger,
                )

                if update_payload:
                    mappings_to_update.append(update_payload)
        else:
            # this canon exists, OR we've already created it for this URL
            # let's check if the package is linked to anything
            if linked_canon_id is None:
                # time to create a new canon package
                new_canon_package = CanonPackage(
                    id=uuid4(),
                    canon_id=actual_canon_id,
                    package_id=pkg_id,
                    created_at=now,
                    updated_at=now,
                )
                mappings_to_create.append(new_canon_package)

            # what if it's linked to something, that's not actual_canon_id
            elif linked_canon_id != actual_canon_id:
                # time to update the existing canon package row
                update_payload = build_update_payload(
                    current_canon_packages,
                    pkg_id,
                    actual_canon_id,
                    now,
                    logger,
                )

                if update_payload:
                    mappings_to_update.append(update_payload)
            else:
                # in this case, no changes needed!
                continue

    # 5. Apply changes
    logger.log("-" * 100)
    logger.log("Changes to apply:")
    logger.log(f"  Canons to create: {len(canons_to_create)}")
    logger.log(f"  Mappings to create: {len(mappings_to_create)}")
    logger.log(f"  Mappings to update: {len(mappings_to_update)}")
    logger.log("-" * 100)

    if not LOAD:
        logger.log("Skipping changes because LOAD is not set")
        return

    db.ingest(list(canons_to_create.values()), mappings_to_create, mappings_to_update)

    logger.log("✅ Deduplication process completed")

    if non_canonical_urls:
        non_canonical_urls_str = "\n".join(
            [f"{url.id}: {url.url}" for url in non_canonical_urls]
        )
        msg = f"""Skipped non_canonical URLs to avoid loading bad data into canons:
        {non_canonical_urls_str}
        """
        logger.warn(msg)


def profile_main(db: DedupeDB):
    cProfile.run("main(db)", "ranker.dedupe_v2.prof")
    p = pstats.Stats("ranker.dedupe_v2.prof")
    p.sort_stats("cumulative")
    p.print_stats()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", action="store_true")
    args = parser.parse_args()

    config: Config = load_config()
    db: DedupeDB = DedupeDB(config)

    try:
        if args.profile:
            profile_main(db)
        else:
            main(db)
    finally:
        db.close()
