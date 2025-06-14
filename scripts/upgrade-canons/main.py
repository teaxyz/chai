#!/usr/bin/env uv run --with psycopg2==2.9.10 --with permalint==0.1.14

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


# let's make classes defining the data models, since scripts can't really access ./core
@dataclass
class URL:
    id: UUID
    url: str
    url_type_id: UUID
    created_at: datetime
    updated_at: datetime


@dataclass
class PackageURL:
    id: UUID
    package_id: UUID
    url_id: UUID
    created_at: datetime
    updated_at: datetime


class DB:
    def __init__(self):
        self.conn = psycopg2.connect(CHAI_DATABASE_URL)
        self.cursor = self.conn.cursor()
        register_uuid(self.conn)

    def get_all_homepages(self):
        """
        Returns a set of all homepage URLs, and a map of package ID to list of homepage
        URLs
        """
        self.cursor.execute("""
            SELECT 
                u.id, 
                u.url, 
                pu.package_id
            FROM package_urls pu 
            JOIN urls u ON pu.url_id = u.id 
            JOIN url_types ut ON ut.id = u.url_type_id 
            WHERE 
                ut.name = 'homepage';""")

        package_url_map: dict[UUID, list[UUID]] = defaultdict(list)
        all_homepages: set[str] = set()

        for url_id, url, package_id in self.cursor.fetchall():
            package_url_map[package_id].append(url_id)
            all_homepages.add(url)

        return all_homepages, package_url_map

    def db_execute_values(
        self, table_name: str, columns: list[str], values: list[tuple]
    ):
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
        """
        inserts into the db using psycopg2's execute_values

        execute_values expects the data to be formatted as a list of tuples
        """
        table_name = "urls"
        columns = ["id", "url", "url_type_id", "created_at", "updated_at"]
        values = [
            (url.id, url.url, url.url_type_id, url.created_at, url.updated_at)
            for url in urls_to_add
        ]
        self.db_execute_values(table_name, columns, values)

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
        self.db_execute_values(table_name, columns, values)

        if not dry_run:
            self.conn.commit()

    def close(self):
        self.cursor.close()
        self.conn.close()


def main(db: DB, homepage_id: UUID, dry_run: bool):
    now = datetime.now()
    print(f"Starting main: {now}")
    new_urls: list[URL] = []
    new_package_urls: list[PackageURL] = []

    print("-" * 100)
    print("Going to insert:")
    print(f"  {len(new_urls)} URLs")
    print(f"  {len(new_package_urls)} PackageURLs")
    print("-" * 100)

    db.ingest(new_urls, new_package_urls, dry_run)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--homepage-id", type=UUID, required=True)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    db = DB()
    try:
        with warnings.catch_warnings(action="ignore"):
            main(db, args.homepage_id, args.dry_run)
    finally:
        db.close()
