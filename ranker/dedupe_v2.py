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
                .order_by(Package.id, URL.created_at.desc())  # Latest URL / package
                .all()
            )

    def ingest(
        self,
        new_canons: list[Canon],
        new_canon_packages: list[CanonPackage],
        updated_canon_packages: list[tuple[UUID, UUID]],
    ) -> None:
        with self.session() as session:
            self.add_with_flush(session, new_canons)
            self.add_with_flush(session, new_canon_packages)
            session.execute(update(CanonPackage), updated_canon_packages)
            session.commit()

    def add_with_flush(self, session: Session, rows: list[BaseModel]) -> None:
        session.add_all(rows)
        session.flush()


def get_latest_homepage_per_package(
    packages_with_homepages: list[tuple[Package, URL]],
) -> dict[UUID, URL]:
    """Get the latest homepage URL for each package."""
    latest_homepages: dict[UUID, URL] = {}

    for pkg, url in packages_with_homepages:
        # Since we ordered by Package.id, URL.created_at.desc(),
        # the first URL we see for each package is the latest
        if pkg.id not in latest_homepages:
            # guard against non-canonicalized URLs
            if not is_canonical_url(url.url):
                raise Exception(f"{url.id}: {url.url} is not canonicalized")
            latest_homepages[pkg.id] = url

    return latest_homepages


def main(config: Config, db: DedupeDB):
    logger = Logger("ranker.dedupe_v2")
    now = datetime.now()
    logger.debug(f"Starting deduplication process at {now}")

    # 1. Get current state
    current_canons: dict[UUID, Canon] = db.get_current_canons()
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

    # 3. Process changes differentially
    canons_to_create: list[Canon] = []
    mappings_to_update: list[tuple[UUID, UUID]] = []  # (id, new_canon_id)
    mappings_to_create: list[CanonPackage] = []

    for pkg_id, url in latest_homepages.items():
        # does the url have a canon?
        actual_canon: Canon | None = current_canons.get(url.id)
        actual_canon_id: UUID | None = actual_canon.id if actual_canon else None

        # is the package tied to a canon?
        linked_canon_id: UUID | None = current_canon_packages.get(pkg_id)

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
                mappings_to_create.append(new_canon_package)
            else:
                # update the existing canon package row
                mappings_to_update.append((pkg_id, new_canon.id))
        else:
            # ok, this is an existing canon
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
                mappings_to_update.append((pkg_id, actual_canon_id))
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

    db.ingest(canons_to_create, mappings_to_create, mappings_to_update)

    logger.log("✅ Deduplication process completed")


if __name__ == "__main__":
    config: Config = load_config()
    db: DedupeDB = DedupeDB(config)
    main(config, db)
