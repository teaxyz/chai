#!/usr/bin/env pkgx +python@3.11 uv run --with psycopg2==2.9.9

import argparse
import csv
import io
import os
import uuid
from datetime import datetime
from typing import Dict

import psycopg2

from core.config import Config, PackageManager
from core.db import DB
from core.logger import Logger

CHAI_DATABASE_URL = os.environ.get("CHAI_DATABASE_URL")
BATCH_SIZE = 20000  # Process this many records at a time


class ChaiDB:
    """Handles all interactions with the CHAI database."""

    def __init__(self):
        """Initialize connection to the CHAI database."""
        self.logger = Logger("chai_db")
        self.conn = psycopg2.connect(CHAI_DATABASE_URL)
        self.cursor = self.conn.cursor()
        self.logger.log("CHAI database connection established")

        # Initialize configuration to get URL types
        self.config = Config(PackageManager.NPM)

        # Configure columns for package_urls
        self.package_url_columns = [
            "id",
            "package_id",
            "url_id",
            "created_at",
            "updated_at",
        ]

        # Initialize caches
        self.url_id_cache = self._load_url_cache()
        self.package_id_cache = self._load_package_cache()
        self.processed_pairs = set()

        self.logger.log(
            f"Loaded {len(self.url_id_cache)} URLs and {len(self.package_id_cache)} packages into cache"
        )

    def _load_url_cache(self) -> Dict[str, uuid.UUID]:
        """Load all URLs into a cache for quick lookups."""
        query = "SELECT url, id FROM urls"
        self.cursor.execute(query)
        return {row[0]: row[1] for row in self.cursor.fetchall()}

    def _load_package_cache(self) -> Dict[str, uuid.UUID]:
        """Load all packages into a cache for quick lookups by import_id."""
        query = "SELECT import_id, id FROM packages"
        self.cursor.execute(query)
        return {row[0]: row[1] for row in self.cursor.fetchall()}

    def get_or_create_url(self, url: str, url_type_id: uuid.UUID) -> uuid.UUID:
        """Get URL ID from cache or create a new URL entry if it doesn't exist."""
        if not url:
            return None

        # Check if URL already exists in cache
        if url in self.url_id_cache:
            return self.url_id_cache[url]

        # URL doesn't exist, create it
        query = """
            INSERT INTO urls (url, url_type_id) 
            VALUES (%s, %s) 
            ON CONFLICT (url_type_id, url) DO UPDATE SET updated_at = NOW()
            RETURNING id
        """
        self.cursor.execute(query, (url, url_type_id))
        url_id = self.cursor.fetchone()[0]
        self.conn.commit()

        # Add to cache
        self.url_id_cache[url] = url_id
        return url_id

    def get_package_id(self, import_id: str) -> uuid.UUID:
        """Get package ID from cache based on import ID."""
        # Check cache for the package ID
        if import_id in self.package_id_cache:
            return self.package_id_cache[import_id]

        return None

    def init_copy_expert(self) -> None:
        """Initialize a StringIO object to collect CSV data for copy operation."""
        self.csv_data = io.StringIO()
        self.columns_str = ", ".join(self.package_url_columns)
        # self.logger.log("Copy buffer initialized")

    def add_row_to_copy_expert(self, package_id: uuid.UUID, url_id: uuid.UUID) -> bool:
        """Add a row to the StringIO buffer for later COPY operation."""
        if not package_id or not url_id:
            return False

        # Check if this pair has already been processed
        if (package_id, url_id) in self.processed_pairs:
            return False

        # Add to processed pairs
        self.processed_pairs.add((package_id, url_id))

        # Add to CSV buffer
        row_id = str(uuid.uuid4())
        created_at = datetime.now().isoformat()
        updated_at = created_at
        csv_line = f"{row_id},{package_id},{url_id},{created_at},{updated_at}"
        self.csv_data.write(csv_line + "\n")
        return True

    def add_rows_with_flush(self, max_buffer_size=100000) -> None:
        """Flush the buffer if it's too large."""
        if self.csv_data.tell() > max_buffer_size:
            self.complete_copy_expert()
            self.init_copy_expert()

    def complete_copy_expert(self) -> None:
        """Execute the COPY operation with collected data."""
        if self.csv_data.tell() == 0:
            self.logger.log("No data to copy")
            return

        # Reset buffer position to start
        self.csv_data.seek(0)

        # Execute the COPY FROM operation
        try:
            self.cursor.copy_expert(
                f"COPY package_urls_temp_import ({self.columns_str}) FROM STDIN WITH CSV",
                self.csv_data,
            )
            self.conn.commit()
            self.logger.log("Data copied to database")
        except psycopg2.Error as e:
            self.logger.log(f"Error copying data to database: {e}")
            # Write the csv data to a file for debugging
            with open("bad_copy_file.csv", "w") as f:
                f.write(self.csv_data.getvalue())
            self.conn.rollback()
            raise e


def process_url_file(file_path: str, stop: int = None) -> None:
    """Process URLs from CSV file and insert into package_urls table.

    Args:
        file_path: Path to the CSV file containing URL data
        stop: Optional number of rows to process for testing
    """
    logger = Logger("url_processor")
    chai_db = ChaiDB()

    # Initialize the copy expert
    chai_db.init_copy_expert()

    logger.log("Starting URL processing")
    try:
        total_rows = 0
        processed_rows = 0

        with open(file_path, "r") as csvfile:
            reader = csv.reader(csvfile)
            # Skip header if exists
            _ = next(reader, None)

            for row in reader:
                total_rows += 1

                # Extract data from row
                if len(row) >= 3:
                    import_id, source, homepage = row[0], row[1], row[2]
                else:
                    logger.log(f"Invalid row format: {row}")
                    continue

                # Get package ID
                package_id = chai_db.get_package_id(import_id)
                if not package_id:
                    # because some npm projects were spam, we didn't add to CHAI
                    continue

                # Get or create URL for homepage
                homepage_url_id = chai_db.get_or_create_url(
                    homepage, chai_db.config.url_types.homepage
                )
                source_url_id = chai_db.get_or_create_url(
                    source, chai_db.config.url_types.source
                )

                # Add to package_urls
                if chai_db.add_row_to_copy_expert(package_id, homepage_url_id):
                    processed_rows += 1

                if chai_db.add_row_to_copy_expert(package_id, source_url_id):
                    processed_rows += 1

                # Periodically flush buffer
                if processed_rows % 20000 == 0:
                    chai_db.add_rows_with_flush()

                # Log progress
                if total_rows % 10000 == 0:
                    logger.log(
                        f"Processed {total_rows} rows, added {processed_rows} relationships"
                    )

                # Break if reached stop limit
                if stop and processed_rows >= stop:
                    break

        # Complete final copy operation
        chai_db.complete_copy_expert()
        logger.log(
            f"URL processing complete. Processed {total_rows} rows, added {processed_rows} relationships"
        )

    except Exception as e:
        logger.log(f"Error processing URLs: {e}")
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load URLs into package_urls table")
    parser.add_argument("file_path", help="Path to CSV file containing URL data")
    parser.add_argument(
        "--stop", type=int, help="Stop after processing this many records"
    )
    args = parser.parse_args()

    process_url_file(args.file_path, args.stop)
