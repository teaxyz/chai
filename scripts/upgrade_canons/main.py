#!/usr/bin/env uv run --with psycopg2==2.9.10 --with permalint==0.1.14

import argparse
import warnings
from datetime import datetime
from uuid import UUID, uuid4

from permalint import is_canonical_url, normalize_url

from scripts.upgrade_canons.db import DB
from scripts.upgrade_canons.structs import URL, PackageURL

# Valid URL types that can be canonicalized
VALID_URL_TYPES = {"homepage", "repository", "source"}


def is_one_url_canonical(urls: list[str]) -> bool:
    """Returns True if at least one of the URLs is canonical"""
    return any(is_canonical_url(url) for url in urls)


def generate_canonical_url(urls: list[str]) -> str:
    """
    Returns the canonical URL for the given list of URLs

      - TODO: we should be smart about which one to pick, like most recent perhaps?
    """
    return normalize_url(urls[0])


def generate_new_url(url: str, url_type_id: UUID, now: datetime) -> URL:
    """Creates a new URL object for the given URL."""
    return URL(uuid4(), url, url_type_id, now, now)


def generate_new_package_url(
    package_id: UUID, url_id: UUID, now: datetime
) -> PackageURL:
    """Creates a new PackageURL object for the given package and URL"""
    return PackageURL(uuid4(), package_id, url_id, now, now)


# Pure functions for business logic - highly testable
def analyze_packages_needing_canonicalization(
    package_url_map: dict[UUID, list[str]],
    existing_urls: set[str],
) -> dict[UUID, str]:
    """
    Analyze which packages need canonical URLs created.
    Returns a mapping of package_id to the canonical URL that should be created.
    """
    packages_needing_canon: dict[UUID, str] = {}
    canonical_urls_to_create: set[str] = set()

    for package_id, urls in package_url_map.items():
        # Skip if package already has at least one canonical URL
        if is_one_url_canonical(urls):
            continue

        canonical_url = generate_canonical_url(urls)

        # Skip if canonical URL already exists in database
        if canonical_url in existing_urls:
            continue

        # Skip if we're already planning to create this canonical URL
        if canonical_url in canonical_urls_to_create:
            continue

        # This package needs a canonical URL created
        packages_needing_canon[package_id] = canonical_url
        canonical_urls_to_create.add(canonical_url)

    return packages_needing_canon


def create_url_and_package_url_objects(
    packages_needing_canon: dict[UUID, str],
    url_type_id: UUID,
    now: datetime,
) -> tuple[list[URL], list[PackageURL]]:
    """
    Create URL and PackageURL objects for the packages that need canonicalization.
    """
    new_urls: list[URL] = []
    new_package_urls: list[PackageURL] = []

    for package_id, canonical_url in packages_needing_canon.items():
        new_url = generate_new_url(canonical_url, url_type_id, now)
        new_package_url = generate_new_package_url(package_id, new_url.id, now)

        new_urls.append(new_url)
        new_package_urls.append(new_package_url)

    return new_urls, new_package_urls


def main(db: DB, url_type: str, url_type_id: UUID, dry_run: bool):
    now = datetime.now()
    print(f"Starting main for URL type '{url_type}': {now}")

    # Get data from database
    all_urls, package_url_map = db.get_urls_by_type(url_type)
    print(f"Found {len(all_urls)} {url_type} URLs")
    print(f"Found {len(package_url_map)} packages with {url_type} URLs")

    # Analyze which packages need canonicalization
    packages_needing_canon = analyze_packages_needing_canonicalization(
        package_url_map, all_urls
    )

    # Create objects
    new_urls, new_package_urls = create_url_and_package_url_objects(
        packages_needing_canon, url_type_id, now
    )

    print("-" * 100)
    print("Going to insert:")
    print(f"  {len(new_urls)} URLs")
    print(f"  {len(new_package_urls)} PackageURLs")
    print("-" * 100)

    # Ingest to database
    db.ingest(new_urls, new_package_urls, dry_run)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Canonicalize URLs of specified type")
    parser.add_argument(
        "--url-type",
        type=str,
        required=True,
        choices=VALID_URL_TYPES,
        help=f"Type of URLs to canonicalize. Valid options: {', '.join(VALID_URL_TYPES)}",
    )
    parser.add_argument(
        "--url-type-id",
        type=UUID,
        required=True,
        help="UUID of the URL type in the database",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run in dry-run mode without making database changes",
    )
    args = parser.parse_args()

    db = DB()
    try:
        with warnings.catch_warnings(action="ignore"):
            main(db, args.url_type, args.url_type_id, args.dry_run)
    finally:
        db.close()
