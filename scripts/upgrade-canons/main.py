#!/usr/bin/env uv run --with psycopg2==2.9.10 --with permalint==0.1.11

import argparse
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from os import getenv
from uuid import UUID, uuid4
import warnings

import psycopg2
from permalint import is_canonical_url, normalize_url
from psycopg2.extras import execute_values, register_uuid

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
class Canon:
    id: UUID
    name: str
    url: str
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

    def get_canons(self) -> list[tuple[UUID, str]]:
        self.cursor.execute("SELECT id, url FROM canons")
        return self.cursor.fetchall()

    def get_homepage_package_urls(self, url_ids: tuple[UUID]) -> dict[UUID, list[UUID]]:
        self.cursor.execute(
            """
                SELECT package_id, url_id 
                FROM package_urls pu 
                JOIN urls u ON u.id = pu.url_id 
                JOIN url_types ut ON ut.id = u.url_type_id 
                WHERE ut.name = 'homepage' AND pu.url_id IN %s""",
            (url_ids,),
        )
        result: dict[UUID, list[UUID]] = defaultdict(list)
        for package_id, url_id in self.cursor.fetchall():
            result[url_id].append(package_id)
        return result

    def get_urls_by_type(self, url_type_id: UUID) -> dict[str, UUID]:
        self.cursor.execute(
            "SELECT id, url FROM urls WHERE url_type_id = %s", (url_type_id,)
        )
        return {url: id for id, url in self.cursor.fetchall()}

    def ingest(self, urls_to_add: list[URL], package_urls_to_add: list[PackageURL]):
        self.cursor.executemany(
            "INSERT INTO urls (id, url, url_type_id, created_at, updated_at) VALUES (%s, %s, %s, %s, %s)",
            [
                (url.id, url.url, url.url_type_id, url.created_at, url.updated_at)
                for url in urls_to_add
            ],
        )
        print(f"Inserted {len(urls_to_add)} URLs")
        self.cursor.executemany(
            "INSERT INTO package_urls (id, package_id, url_id, created_at, updated_at) VALUES (%s, %s, %s, %s, %s)",
            [
                (
                    package_url.id,
                    package_url.package_id,
                    package_url.url_id,
                    package_url.created_at,
                    package_url.updated_at,
                )
                for package_url in package_urls_to_add
            ],
        )
        print(f"Inserted {len(package_urls_to_add)} package URLs")
        self.conn.commit()


def main(db: DB, homepage_id: UUID, dry_run: bool):
    now = datetime.now()

    # get all the existing canons
    canons: list[tuple[UUID, str]] = db.get_canons()
    print(f"Found {len(canons)} existing canons")

    # get all existing URLs
    urls: dict[str, UUID] = db.get_urls_by_type(homepage_id)
    print(f"Found {len(urls)} existing URLs")

    # we'll need a map of new URL ID to old URL ID
    old_url_to_new_url: dict[UUID, UUID] = {}
    urls_to_add: list[URL] = []

    for canon_id, url in canons:
        try:
            if not is_canonical_url(url):
                # canonicalize the URL
                canonical_url = normalize_url(url)

                # create the new URL object
                new_url = URL(
                    id=uuid4(),
                    url=canonical_url,
                    url_type_id=homepage_id,
                    created_at=now,
                    updated_at=now,
                )

                # add it to our master list of URLs to add
                urls_to_add.append(new_url)

                # populate the map
                # we'd first need the URL ID of the old URL value
                old_url_id = urls[url]
                old_url_to_new_url[old_url_id] = new_url.id
        except ValueError as e:
            print(f"{canon_id}: {url} is malformed: {e}")
    print(f"⭐️ Populated {len(urls_to_add)} canonical URLs")
    print(f"️️️⭐️ Made a map for {len(old_url_to_new_url)} old URLs")

    # now, for each of the old URLs, we need to know what packages they belong to, so we
    # can replicate those relationships to the new URLs
    old_url_ids = tuple(old_url_to_new_url.keys())
    existing_package_urls: dict[UUID, list[UUID]] = db.get_homepage_package_urls(
        old_url_ids
    )
    print(f"Found {len(existing_package_urls)} existing package URLs")
    new_package_urls: list[PackageURL] = []

    for package_id, url_ids in existing_package_urls.items():
        for url_id in url_ids:
            canonicalized_url_id = old_url_to_new_url.get(url_id)
            if not canonicalized_url_id:
                # this wouldn't happen for non-homepage URLs
                raise ValueError(f"No canonicalized URL ID found for {url_id}")

            # we'll need a new package url object for this
            new_package_url = PackageURL(
                id=uuid4(),
                package_id=package_id,
                url_id=canonicalized_url_id,
                created_at=now,
                updated_at=now,
            )
            new_package_urls.append(new_package_url)

    print("-" * 100)
    print("Going to insert:")
    print(f"  {len(urls_to_add)} URLs")
    print(f"  {len(new_package_urls)} PackageURLs")
    print("-" * 100)

    if not dry_run:
        db.ingest(urls_to_add, new_package_urls)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--homepage-id", type=UUID, required=True)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    db = DB()
    with warnings.catch_warnings(action="ignore"):
        main(db, args.homepage_id, args.dry_run)
