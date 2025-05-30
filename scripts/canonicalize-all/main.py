#!/usr/bin/env uv run --with psycopg2==2.9.10 --with permalint==0.1.11

import argparse
import warnings
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from os import getenv
from uuid import UUID, uuid4

import psycopg2
from permalint import is_canonical_url, normalize_url
from psycopg2.extras import execute_values, register_uuid
from psycopg2.sql import SQL, Identifier

CHAI_DATABASE_URL = getenv("CHAI_DATABASE_URL")

if not CHAI_DATABASE_URL:
    raise Exception("CHAI_DATABASE_URL is not set")


@dataclass
class URL:
    id: UUID
    url: str
    url_type_id: UUID
    created_at: datetime
    updated_at: datetime


@dataclass
class URLType:
    id: UUID
    name: str


@dataclass
class PackageURL:
    id: UUID
    package_id: UUID
    url_id: UUID
    created_at: datetime
    updated_at: datetime


@dataclass
class PackageURLInfo:
    package_id: UUID
    url: str
    url_type_id: UUID
    url_type_name: str


class DB:
    def __init__(self):
        self.conn = psycopg2.connect(CHAI_DATABASE_URL)
        self.cursor = self.conn.cursor()
        register_uuid(self.conn)

    def get_url_types(self) -> dict[UUID, URLType]:
        """Get all URL types."""
        self.cursor.execute("SELECT id, name FROM url_types")
        return {id: URLType(id=id, name=name) for id, name in self.cursor.fetchall()}

    def get_package_urls_by_type(self) -> dict[UUID, list[PackageURLInfo]]:
        """Get all package URLs grouped by package ID."""
        self.cursor.execute("""
            SELECT pu.package_id, u.url, u.url_type_id, ut.name as url_type_name
            FROM package_urls pu 
            JOIN urls u ON u.id = pu.url_id 
            JOIN url_types ut ON ut.id = u.url_type_id 
            ORDER BY pu.package_id, ut.name
        """)

        result: dict[UUID, list[PackageURLInfo]] = defaultdict(list)
        for package_id, url, url_type_id, url_type_name in self.cursor.fetchall():
            result[package_id].append(
                PackageURLInfo(
                    package_id=package_id,
                    url=url,
                    url_type_id=url_type_id,
                    url_type_name=url_type_name,
                )
            )
        return result

    def get_existing_urls_by_type(self, url_type_id: UUID) -> dict[str, UUID]:
        """Get existing URLs for a specific URL type."""
        self.cursor.execute(
            "SELECT url, id FROM urls WHERE url_type_id = %s", (url_type_id,)
        )
        return {url: id for url, id in self.cursor.fetchall()}

    def db_execute_values(
        self, table_name: str, columns: list[str], values: list[tuple]
    ):
        """Execute batch insert using psycopg2's execute_values."""
        if not values:
            print(f"No values to insert into {table_name}")
            return

        query = (
            SQL("INSERT INTO {table_name} ({columns}) VALUES %s")
            .format(
                table_name=Identifier(table_name),
                columns=SQL(", ").join(Identifier(column) for column in columns),
            )
            .as_string(self.conn)
        )
        try:
            execute_values(self.cursor, query, values)
            print(f"Inserted {len(values)} rows into {table_name}")
        except Exception as e:
            print(f"Error inserting {table_name}: {e}")
            raise

    def ingest(
        self,
        urls_to_add: list[URL],
        package_urls_to_add: list[PackageURL],
        dry_run: bool,
    ):
        """Insert URLs and PackageURLs into the database."""
        if urls_to_add:
            table_name = "urls"
            columns = ["id", "url", "url_type_id", "created_at", "updated_at"]
            values = [
                (url.id, url.url, url.url_type_id, url.created_at, url.updated_at)
                for url in urls_to_add
            ]
            if not dry_run:
                self.db_execute_values(table_name, columns, values)
            else:
                print(f"[DRY RUN] Would insert {len(values)} URLs")

        if package_urls_to_add:
            table_name = "package_urls"
            columns = ["id", "package_id", "url_id", "created_at", "updated_at"]
            values = [
                (
                    package_url.id,
                    package_url.package_id,
                    package_url.url_id,
                    package_url.created_at,
                    package_url.updated_at,
                )
                for package_url in package_urls_to_add
            ]
            if not dry_run:
                self.db_execute_values(table_name, columns, values)
            else:
                print(f"[DRY RUN] Would insert {len(values)} PackageURLs")

        if not dry_run:
            self.conn.commit()

    def close(self):
        self.cursor.close()
        self.conn.close()


def main(db: DB, dry_run: bool):
    now = datetime.now()

    # Get all URL types
    url_types = db.get_url_types()
    print(f"Found {len(url_types)} URL types: {[ut.name for ut in url_types.values()]}")

    # Get all package URLs
    package_urls = db.get_package_urls_by_type()
    print(f"Found {len(package_urls)} packages with URLs")

    # Track what needs to be added
    urls_to_add: list[URL] = []
    package_urls_to_add: list[PackageURL] = []

    # Track statistics
    stats = {
        "packages_processed": 0,
        "url_types_processed": 0,
        "canonical_urls_found": 0,
        "canonical_urls_created": 0,
        "malformed_urls": 0,
        "skipped_existing": 0,
    }

    for package_id, package_url_infos in package_urls.items():
        stats["packages_processed"] += 1

        # Group URLs by type for this package
        urls_by_type: dict[UUID, list[PackageURLInfo]] = defaultdict(list)
        for info in package_url_infos:
            urls_by_type[info.url_type_id].append(info)

        # For each URL type this package has URLs for
        for url_type_id, url_infos in urls_by_type.items():
            stats["url_types_processed"] += 1
            url_type_name = url_infos[0].url_type_name

            # Get existing URLs for this type to avoid duplicates
            existing_urls = db.get_existing_urls_by_type(url_type_id)

            # Check if we already have a canonical URL for this type
            canonical_url_exists = False
            canonical_url = None

            for info in url_infos:
                try:
                    if is_canonical_url(info.url):
                        canonical_url_exists = True
                        canonical_url = info.url
                        stats["canonical_urls_found"] += 1
                        print(f"  ✓ Already has canonical URL: {info.url}")
                        break
                except ValueError as e:
                    print(f"  ⚠️  Malformed URL: {info.url} - {e}")
                    stats["malformed_urls"] += 1
                    continue

            # If no canonical URL exists, create one from the first valid URL
            if not canonical_url_exists:
                for info in url_infos:
                    try:
                        normalized = normalize_url(info.url)

                        # Check if this normalized URL already exists
                        if normalized in existing_urls:
                            print(f"  ⭐ Normalized URL already exists: {normalized}")
                            stats["skipped_existing"] += 1
                            # Still need to create PackageURL relationship if it doesn't exist
                            existing_url_id = existing_urls[normalized]
                            package_url = PackageURL(
                                id=uuid4(),
                                package_id=package_id,
                                url_id=existing_url_id,
                                created_at=now,
                                updated_at=now,
                            )
                            package_urls_to_add.append(package_url)
                            canonical_url = normalized
                            break
                        else:
                            # Create new canonical URL
                            canonical_url = normalized
                            new_url = URL(
                                id=uuid4(),
                                url=canonical_url,
                                url_type_id=url_type_id,
                                created_at=now,
                                updated_at=now,
                            )
                            urls_to_add.append(new_url)

                            # Create PackageURL relationship
                            package_url = PackageURL(
                                id=uuid4(),
                                package_id=package_id,
                                url_id=new_url.id,
                                created_at=now,
                                updated_at=now,
                            )
                            package_urls_to_add.append(package_url)

                            stats["canonical_urls_created"] += 1
                            print(f"  ✨ Created canonical URL: {canonical_url}")
                            break

                    except ValueError as e:
                        print(f"  ⚠️  Malformed URL: {info.url} - {e}")
                        stats["malformed_urls"] += 1
                        continue

    print("\n" + "=" * 100)
    print("CANONICALIZATION SUMMARY")
    print("=" * 100)
    print(f"Packages processed: {stats['packages_processed']}")
    print(f"URL types processed: {stats['url_types_processed']}")
    print(f"Canonical URLs already existed: {stats['canonical_urls_found']}")
    print(f"Canonical URLs to be created: {stats['canonical_urls_created']}")
    print(f"Normalized URLs already existed: {stats['skipped_existing']}")
    print(f"Malformed URLs skipped: {stats['malformed_urls']}")
    print(f"Total URLs to add: {len(urls_to_add)}")
    print(f"Total PackageURLs to add: {len(package_urls_to_add)}")
    print("=" * 100)

    if dry_run:
        print("\n🔍 DRY RUN MODE - No changes will be made to the database")
        if urls_to_add:
            print(f"\nSample URLs that would be created:")
            for i, url in enumerate(urls_to_add[:5]):
                url_type_name = next(
                    ut.name for ut in url_types.values() if ut.id == url.url_type_id
                )
                print(f"  {i+1}. {url.url} ({url_type_name})")
            if len(urls_to_add) > 5:
                print(f"  ... and {len(urls_to_add) - 5} more")
    else:
        print("\n💾 Inserting data into database...")

    # Insert the data
    db.ingest(urls_to_add, package_urls_to_add, dry_run)

    if not dry_run:
        print("✅ Canonicalization complete!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Canonicalize URLs for all packages across all URL types"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview changes without modifying the database",
    )
    args = parser.parse_args()

    db = DB()
    try:
        with warnings.catch_warnings(action="ignore"):
            main(db, args.dry_run)
    finally:
        db.close()
