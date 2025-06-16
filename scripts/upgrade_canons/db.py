from collections import defaultdict
from os import getenv
from uuid import UUID

import psycopg2
from psycopg2.extras import execute_values, register_uuid
from psycopg2.sql import SQL, Identifier

from scripts.upgrade_canons.structs import URL, PackageURL

CHAI_DATABASE_URL = getenv("CHAI_DATABASE_URL")


class DB:
    def __init__(self):
        if not CHAI_DATABASE_URL:
            raise Exception("CHAI_DATABASE_URL is not set")

        self.conn = psycopg2.connect(CHAI_DATABASE_URL)
        self.cursor = self.conn.cursor()
        register_uuid(self.conn)

    def get_all_homepages(self) -> tuple[set[str], dict[UUID, list[str]]]:
        """
        Returns a set of ALL homepage URLs (including orphans), and a map of package ID
        to list of homepage URL strings for URLs that are attached to packages
        """
        self.cursor.execute("""
            SELECT 
                u.url, 
                pu.package_id
            FROM urls u 
            JOIN url_types ut ON ut.id = u.url_type_id 
            LEFT JOIN package_urls pu ON pu.url_id = u.id 
            WHERE 
                ut.name = 'homepage';""")

        package_url_map: dict[UUID, list[str]] = defaultdict(list)
        all_homepages: set[str] = set()

        for url, package_id in self.cursor.fetchall():
            all_homepages.add(url)  # Add all URLs (including orphans)
            if (
                package_id is not None
            ):  # Only add to package map if attached to a package
                package_url_map[package_id].append(url)

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
        if urls_to_add:
            table_name = "urls"
            columns = ["id", "url", "url_type_id", "created_at", "updated_at"]
            values = [
                (url.id, url.url, url.url_type_id, url.created_at, url.updated_at)
                for url in urls_to_add
            ]
            self.db_execute_values(table_name, columns, values)

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
            self.db_execute_values(table_name, columns, values)

        if not dry_run:
            self.conn.commit()

    def close(self):
        self.cursor.close()
        self.conn.close()

        if self.app_conn:
            self.app_cursor.close()
            self.app_conn.close()
