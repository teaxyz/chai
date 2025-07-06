#!/usr/bin/env pkgx uv run

import argparse
import csv
import sys
import warnings
from uuid import UUID

from permalint import is_canonical_url

from scripts.upgrade_canons.db import DB


def write_to_csv(filename: str, headers: list[str], data: list[tuple]):
    with open(filename, "w") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(data)


def get_all_urls(db: DB) -> list[tuple[UUID, str]]:
    """
    Query all URLs from the urls table.
    Returns list of tuples (url_id, url_string).
    """
    db.cursor.execute(
        """
        SELECT id, url
        FROM urls
        ORDER BY id
        """
    )
    return db.cursor.fetchall()


def identify_non_canonical_urls(urls: list[tuple[UUID, str]]) -> list[UUID]:
    """
    Check each URL for canonicality using permalint.
    Returns list of URL IDs that are not canonical.
    """
    non_canonical_ids = []

    for url_id, url_string in urls:
        try:
            if not is_canonical_url(url_string):
                non_canonical_ids.append(url_id)
        except Exception as e:
            print(f"Warning: Error checking URL {url_string}: {e}")
            # Treat URLs that can't be checked as non-canonical
            non_canonical_ids.append(url_id)

    return non_canonical_ids


def delete_urls_from_database(db: DB, url_ids: list[UUID], dry_run: bool) -> None:
    """
    Delete URLs and their package_urls entries from the database.
    """
    if not url_ids:
        print("No URLs to delete.")
        return

    if dry_run:
        print(
            f"DRY RUN: Would delete {len(url_ids)} URLs and their package_urls entries"
        )
        return

    # Batch delete operations for efficiency
    placeholders = ",".join(["%s"] * len(url_ids))

    # Delete from canons first (if any exist)
    db.cursor.execute(f"DELETE FROM canons WHERE url_id IN ({placeholders})", url_ids)

    # Delete from package_urls (foreign key constraint)
    db.cursor.execute(
        f"DELETE FROM package_urls WHERE url_id IN ({placeholders})", url_ids
    )

    # Then delete from urls
    db.cursor.execute(f"DELETE FROM urls WHERE id IN ({placeholders})", url_ids)

    # Commit the transaction
    db.conn.commit()
    print(f"Successfully deleted {len(url_ids)} URLs and their package_urls entries")


def main(dry_run: bool = False):
    """Main function to delete non-canonical URLs."""
    print("Starting deletion of non-canonical URLs...")

    db = DB()
    try:
        # Get all URLs from database
        print("Fetching all URLs from database...")
        all_urls = get_all_urls(db)
        print(f"Found {len(all_urls)} total URLs")

        # Identify non-canonical URLs
        print("Checking URLs for canonicality...")
        non_canonical_ids = identify_non_canonical_urls(all_urls)
        canonical_count = len(all_urls) - len(non_canonical_ids)

        print(f"Found {len(non_canonical_ids)} non-canonical URLs")
        print(f"Found {canonical_count} canonical URLs")

        # Delete non-canonical URLs
        if non_canonical_ids:
            canons = db.get_canons_by_url_ids(non_canonical_ids)

            if canons:
                print(f"WARNING: Found {len(canons)} - delete them urself")
                write_to_csv(
                    "non_canonical_urls_that_have_canons.csv",
                    ["canon_id", "url_id"],
                    canons,
                )
                sys.exit(1)

            print("Deleting non-canonical URLs...")
            delete_urls_from_database(db, non_canonical_ids, dry_run)
        else:
            print("No non-canonical URLs found to delete.")

        # Summary
        print("-" * 50)
        if dry_run:
            print("DRY RUN SUMMARY:")
            print(f"Would delete: {len(non_canonical_ids)} URLs")
            print(f"Would keep: {canonical_count} URLs")
        else:
            print("DELETION SUMMARY:")
            print(f"✅ Deleted: {len(non_canonical_ids)} URLs")
            print(f"✅ Kept: {canonical_count} URLs")
        print("-" * 50)

    finally:
        db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Delete non-canonical URLs from the database"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run in dry-run mode without making database changes",
    )
    args = parser.parse_args()

    with warnings.catch_warnings(action="ignore"):
        main(args.dry_run)
