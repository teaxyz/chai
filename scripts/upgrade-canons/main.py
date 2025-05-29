#!/usr/bin/env uv run --with psycopg2==2.9.10 --with permalint==0.1.10

import argparse
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from os import getenv
from uuid import UUID, uuid4

import psycopg2
from permalint import is_canonical_url

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

    def get_canons(self) -> list[tuple[UUID, str]]:
        self.cursor.execute("SELECT id, url FROM canons")
        return self.cursor.fetchall()

    def get_package_urls(self, url_strings: list[str]) -> dict[UUID, list[UUID]]:
        self.cursor.execute(
            "SELECT package_id, url_id FROM package_urls WHERE url IN %s",
            (url_strings,),
        )
        result: dict[UUID, list[UUID]] = defaultdict(list)
        for package_id, url_id in self.cursor.fetchall():
            result[url_id].append(package_id)
        return result

    def ingest(
        self,
        urls_to_add: list[URL],
        package_urls_to_add: list[PackageURL],
        canons_to_update: list[Canon],
    ): ...


def main(db: DB, homepage_id: UUID):
    now = datetime.now()
    # get all the canons
    canons: list[tuple[UUID, str]] = db.get_canons()

    urls_to_add: list[URL] = []
    package_urls_to_add: list[PackageURL] = []

    for canon_id, url in canons:
        try:
            if not is_canonical_url(url):
                # everything in canons is a Homepage
                new_url = URL(
                    id=uuid4(),
                    url=url,
                    url_type_id=homepage_id,
                    created_at=now,
                    updated_at=now,
                )
                urls_to_add.append(new_url)
        except ValueError as e:
            print(f"{canon_id}: {url} is malformed: {e}")

    # now, for each of these urls_to_add, we should copy the existing package_url
    # entries they already have
    package_urls: dict[UUID, list[UUID]] = db.get_package_urls(urls_to_add)

    # package_urls is a dictionary of each exi

    # now check if they are normalized or not
    print(f"Found {len(urls_to_add)} non-canonicalized canons")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--homepage-id", type=UUID, required=True)
    args = parser.parse_args()

    db = DB()
    main(db, args.homepage_id)
