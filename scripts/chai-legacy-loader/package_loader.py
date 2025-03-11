#!/usr/bin/env pkgx +python@3.11 uv run --with sqlalchemy==2.0.34

"""
Legacy CHAI Package Loader

This script loads package data from the legacy CHAI database into
the current CHAI database schema, handling large data volumes efficiently.
"""

import logging
import os
import uuid
from typing import Any, Dict, List

import psycopg2
import sqlalchemy as sa
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Database connection parameters
LEGACY_DB_PARAMS = os.environ.get("LEGACY_CHAI_DATABASE_URL")
CHAI_DB_URL = os.environ.get("CHAI_DATABASE_URL")
if not LEGACY_DB_PARAMS or not CHAI_DB_URL:
    raise ValueError("Legacy CHAI database URL or CHAI database URL not set")

# Constants
BATCH_SIZE = 10000  # Process this many records at a time
NPM_PACKAGE_MANAGER_NAME = "npm"  # Used to look up the package manager ID


class LegacyDB:
    """Handles all interactions with the legacy CHAI database."""

    def __init__(self):
        """Initialize connection to the legacy database."""
        self.conn = psycopg2.connect(**LEGACY_DB_PARAMS)
        # Set autocommit to False for server-side cursors
        self.conn.set_session(autocommit=False)
        logger.info("Legacy database connection established")

    def __del__(self):
        """Close connection when object is destroyed."""
        if hasattr(self, "conn") and self.conn:
            self.conn.close()

    def get_sql_content(self, filename: str) -> str:
        """Load SQL content from a file."""
        sql_file_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "sql", filename
        )
        with open(sql_file_path, "r") as f:
            return f.read()

    def create_server_cursor(self, sql_file: str, cursor_name: str) -> None:
        """Create a server-side cursor for efficient data fetching."""
        query = self.get_sql_content(sql_file)
        cursor = self.conn.cursor()
        # Create a named server-side cursor
        declare_stmt = f"DECLARE {cursor_name} CURSOR FOR {query}"
        cursor.execute(declare_stmt)
        logger.info(f"Created server-side cursor '{cursor_name}' for {sql_file}")
        cursor.close()

    def fetch_batch(self, cursor_name: str, batch_size: int) -> List[tuple]:
        """Fetch a batch of records using the server-side cursor."""
        cursor = self.conn.cursor()
        cursor.execute(f"FETCH {batch_size} FROM {cursor_name}")
        batch = cursor.fetchall()
        logger.info(f"Fetched {len(batch)} records from cursor '{cursor_name}'")
        cursor.close()
        return batch

    def close_cursor(self, cursor_name: str) -> None:
        """Close a server-side cursor."""
        cursor = self.conn.cursor()
        cursor.execute(f"CLOSE {cursor_name}")
        logger.info(f"Closed server-side cursor '{cursor_name}'")
        cursor.close()


class ChaiDB:
    """Handles all interactions with the current CHAI database."""

    def __init__(self):
        """Initialize connection to the CHAI database."""
        self.engine = create_engine(CHAI_DB_URL)
        logger.info("CHAI database connection established")
        self.get_npm_package_manager_id()
        logger.info("NPM package manager ID fetched")

    def get_npm_package_manager_id(self) -> uuid.UUID:
        """Get the UUID of the npm package manager from the database."""
        if self.npm_pm_id:
            return self.npm_pm_id

        with Session(self.engine) as session:
            # First get the source ID for npm
            source_query = sa.text(
                """
                SELECT id FROM sources WHERE type = :source_type
                """
            )
            source_id = session.execute(
                source_query, {"source_type": NPM_PACKAGE_MANAGER_NAME}
            ).scalar_one_or_none()

            if not source_id:
                raise ValueError(
                    f"Source '{NPM_PACKAGE_MANAGER_NAME}' not found in database"
                )

            # Then get the package manager ID for that source
            pm_query = sa.text(
                """
                SELECT id FROM package_managers WHERE source_id = :source_id
                """
            )
            pm_id = session.execute(
                pm_query, {"source_id": source_id}
            ).scalar_one_or_none()

            if not pm_id:
                raise ValueError(
                    f"Package manager for source '{NPM_PACKAGE_MANAGER_NAME}' not found"
                )

            self.npm_pm_id = pm_id
            logger.info(f"Found npm package manager ID: {pm_id}")
            return pm_id

    def insert_packages(self, package_data: List[Dict[str, Any]]) -> int:
        """Insert package data into the CHAI database."""
        with Session(self.engine) as session:
            if not package_data:
                return 0

            insert_query = sa.text(
                """
                INSERT INTO packages 
                (id, derived_id, name, package_manager_id, import_id, created_at, updated_at)
                VALUES 
                (:id, :derived_id, :name, :package_manager_id, :import_id, :created_at, :updated_at)
                ON CONFLICT (derived_id) DO NOTHING
                """
            )
            session.execute(insert_query, package_data)
            session.commit()
            logger.info(f"Inserted {len(package_data)} packages into CHAI")
            return len(package_data)

    def insert_legacy_dependencies(self, data: Any) -> int:
        pass


def load_packages():
    """Main function to load packages from legacy database to CHAI using server-side cursors."""
    logger.info("Starting package loading process")

    # Initialize database handlers
    legacy_db = LegacyDB()
    chai_db = ChaiDB()

    # Get npm package manager ID
    npm_pm_id = chai_db.npm_pm_id

    # Create a server-side cursor for the packages query
    cursor_name = "packages_cursor"
    legacy_db.create_server_cursor("packages.sql", cursor_name)

    # Process in batches
    total_processed = 0
    total_inserted = 0
    batch_num = 0

    try:
        while True:
            # Fetch a batch of packages
            packages = legacy_db.fetch_batch(cursor_name, BATCH_SIZE)
            if not packages:
                logger.info("No more packages to process")
                break

            batch_num += 1
            total_processed += len(packages)
            logger.info(f"Processing batch {batch_num} with {len(packages)} records")

            # Transform the data to match the current schema
            package_data = []
            for derived_key, name, import_id in packages:
                package_data.append(
                    {
                        "derived_id": derived_key,
                        "name": name,
                        "package_manager_id": npm_pm_id,
                        "import_id": import_id,
                    }
                )

            # Insert transformed data into CHAI
            inserted = chai_db.insert_packages(package_data)
            total_inserted += inserted

            logger.info(
                f"Progress: {total_processed} processed, {total_inserted} inserted"
            )
    finally:
        # Ensure cursor is closed even if an error occurs
        legacy_db.close_cursor(cursor_name)
        logger.info(
            f"✅ Completed: {total_processed} processed, {total_inserted} inserted"
        )


def load_dependencies():
    """Load dependencies from the legacy database to the CHAI database."""
    logger.info("Starting dependency loading process")

    # Initialize database handlers
    legacy_db = LegacyDB()
    chai_db = ChaiDB()

    # Create a server-side cursor for the dependencies query
    cursor_name = "dependencies_cursor"
    legacy_db.create_server_cursor("dependencies.sql", cursor_name)

    # Process in batches
    total_processed = 0
    total_inserted = 0
    batch_num = 0
    stop = 100000

    try:
        while True:
            # Fetch a batch of dependencies
            dependencies = legacy_db.fetch_batch(cursor_name, BATCH_SIZE)
            if not dependencies:
                logger.info("No more dependencies to process")
                break

            batch_num += 1
            total_processed += len(dependencies)
            logger.info(
                f"Processing batch {batch_num} with {len(dependencies)} records"
            )
    except Exception as e:
        logger.error(f"Error processing batch {batch_num}: {e}")
        raise
    finally:
        # Ensure cursor is closed even if an error occurs
        legacy_db.close_cursor(cursor_name)
        logger.info(
            f"✅ Completed: {total_processed} processed, {total_inserted} inserted"
        )


if __name__ == "__main__":
    load_packages()
