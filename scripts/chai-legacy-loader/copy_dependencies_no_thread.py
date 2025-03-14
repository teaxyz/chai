#!/usr/bin/env pkgx +python@3.11 uv run
import io
import os
import uuid
from typing import Dict, List
import json

import psycopg2

from core.config import Config, PackageManager
from core.db import DB
from core.logger import Logger
import psycopg2.errors

LEGACY_CHAI_DATABASE_URL = os.environ.get("LEGACY_CHAI_DATABASE_URL")
CHAI_DATABASE_URL = os.environ.get("CHAI_DATABASE_URL")
BATCH_SIZE = 20000  # Process this many records at a time
DEPENDENCY_TYPES_MAP = {"dependency": "runtime", "dev_dependency": "development"}


# just to get the package manager ID
config_db = DB()
config = Config(PackageManager.NPM, config_db)
del config_db


class LegacyDB:
    """Handles all interactions with the legacy CHAI database."""

    def __init__(self):
        """Initialize connection to the legacy database."""
        self.conn = psycopg2.connect(LEGACY_CHAI_DATABASE_URL)
        # Set autocommit to False for server-side cursors
        self.conn.set_session(autocommit=False)
        self.logger = Logger("legacy_db")
        self.logger.log("Legacy database connection established")

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
        self.logger.log(f"Created server-side cursor '{cursor_name}' for {sql_file}")
        cursor.close()

    def fetch_batch(self, cursor_name: str, batch_size: int) -> List[tuple]:
        """Fetch a batch of records using the server-side cursor."""
        cursor = self.conn.cursor()
        cursor.execute(f"FETCH {batch_size} FROM {cursor_name}")
        batch = cursor.fetchall()
        self.logger.log(f"Fetched {len(batch)} records from cursor '{cursor_name}'")
        cursor.close()
        return batch

    def close_cursor(self, cursor_name: str) -> None:
        """Close a server-side cursor."""
        cursor = self.conn.cursor()
        cursor.execute(f"CLOSE {cursor_name}")
        self.logger.log(f"Closed server-side cursor '{cursor_name}'")
        cursor.close()


class ChaiDB:
    """Handles all interactions with the CHAI database."""

    def __init__(self):
        """Initialize connection to the CHAI database."""
        self.logger = Logger("chai_db")
        self.conn = psycopg2.connect(CHAI_DATABASE_URL)
        self.logger.log("CHAI database connection established")

        # create the cursor
        self.cursor = self.conn.cursor()

        # configure some variables
        self.legacy_dependency_columns = [
            "package_id",
            "dependency_id",
            # the below two are not available from the sources table in the legacy db
            # assuming everything is a runtime dependency and use the semver range *
            "dependency_type_id",
            "semver_range",
        ]
        self.npm_map = self.get_npm_map()
        self.logger.log("NPM map initialized")
        self.processed_pairs = set()

    def get_npm_map(self) -> Dict[str, uuid.UUID]:
        """Get a map of npm package names to their IDs."""
        query = """SELECT import_id, id 
            FROM packages 
            WHERE package_manager_id = %(npm_pm_id)s
            AND import_id IS NOT NULL"""
        self.cursor.execute(query, {"npm_pm_id": config.pm_config.pm_id})
        return {row[0]: row[1] for row in self.cursor.fetchall()}

    def init_copy_expert(self) -> None:
        """Initialize a StringIO object to collect CSV data for copy operation"""
        self.csv_data = io.StringIO()
        self.columns_str = ", ".join(self.legacy_dependency_columns)
        self.logger.log("Copy buffer initialized")

    def get_dependency_type(self, dependency_type: str):
        match dependency_type:
            case "dependency":
                return config.dependency_types.runtime
            case "dev_dependency":
                return config.dependency_types.development
            case _:
                raise ValueError(f"Invalid dependency type: {dependency_type}")

    def handle_semver(self, semver_range) -> str:
        """Process semver range value to handle special cases.
        
        Handles two specific scenarios:
        1. JSON strings: Extracts the 'version' field from JSON objects
        2. CSV formatting: Properly quotes semver ranges containing commas
        
        Args:
            semver_range: The raw semver range value from the database
            
        Returns:
            Processed semver range ready for CSV inclusion
        """
        # Handle case where semver_range is a JSON string
        if semver_range and isinstance(semver_range, str) and semver_range.startswith('{'):
            try:
                # Try to parse as JSON
                semver_json = json.loads(semver_range)
                # If it's a JSON object with a version field, use that
                if isinstance(semver_json, dict) and 'version' in semver_json:
                    semver_range = semver_json['version']
            except json.JSONDecodeError:
                # If it's not valid JSON, keep it as is
                pass
                
        # Escape semver_range to ensure it works as a CSV field
        if semver_range and ',' in str(semver_range):
            semver_range = f'"{semver_range}"'
            
        return semver_range

    def add_rows_to_copy_expert(self, rows: List[tuple]) -> int:
        """Add rows to the StringIO buffer for later COPY operation"""
        rows_added = 0
        for row in rows:
            package_id = self.npm_map.get(row[0])
            dependency_id = self.npm_map.get(row[1])

            # if package or dependency are not found, skip the row
            if not package_id or not dependency_id:
                continue

            # if the pair has already been processed, skip the row
            if (package_id, dependency_id) in self.processed_pairs:
                continue

            # add the pair to the processed pairs
            self.processed_pairs.add((package_id, dependency_id))

            # get the dependency type and semver range
            # not available from the sources table in the legacy db
            # assume everything is a runtime dependency
            # dependency_type_id = self.get_dependency_type(row[2])
            # semver_range = self.handle_semver(row[3])
            dependency_type_id = config.dependency_types.runtime
            semver_range = "*"

            csv_line = (
                f"{package_id},{dependency_id},{dependency_type_id},{semver_range}"
            )
            self.csv_data.write(csv_line + "\n")
            rows_added += 1

        return rows_added

    def add_rows_with_flush(self, rows: List[tuple], max_buffer_size=100000) -> int:
        """Add rows to the StringIO buffer for later COPY operation"""
        rows_added = self.add_rows_to_copy_expert(rows)

        # if the buffer is too large, flush it
        if self.csv_data.tell() > max_buffer_size:
            self.complete_copy_expert()
            # reinitialize the buffer
            self.init_copy_expert()

        return rows_added

    def complete_copy_expert(self):
        """Execute the COPY operation with collected data"""
        # Reset buffer position to start
        self.csv_data.seek(0)

        # Execute the COPY FROM operation
        try:
            self.cursor.copy_expert(
                f"COPY legacy_dependencies ({self.columns_str}) FROM STDIN WITH CSV",
                self.csv_data,
            )
            self.conn.commit()
            self.logger.log("Data copied to database")
        except psycopg2.errors.BadCopyFileFormat as e:
            self.logger.log(f"Error copying data to database: {e}")
            # write the csv data to a file
            with open("bad_copy_file.csv", "w") as f:
                f.write(self.csv_data.getvalue())
            self.conn.rollback()
            raise e


if __name__ == "__main__":
    logger = Logger("chai_legacy_loader")
    legacy_db = LegacyDB()
    chai_db = ChaiDB()

    # initialize the copy expert
    chai_db.init_copy_expert()

    # set up the legacy db
    cursor_name = "legacy_dependencies_cursor"
    legacy_db.create_server_cursor("dependencies.sql", cursor_name)

    # stop mode
    stop = None

    logger.log("Starting dependency copy")
    try:
        total_rows = 0
        while True:
            rows = legacy_db.fetch_batch(cursor_name, BATCH_SIZE)

            # break if we have no more rows
            if not rows:
                break

            # keep adding the rows to the copy expert
            rows_added = chai_db.add_rows_with_flush(rows)
            logger.log(f"Added {rows_added} rows to the copy expert")

            # update the total rows processed
            total_rows += rows_added

            # break if we have processed the stop number of rows
            if stop and total_rows >= stop:
                break

        # complete the copy expert
        logger.log("Attempting to complete the copy expert")
        chai_db.complete_copy_expert()
        logger.log(f"Total rows processed: {total_rows}")

    except KeyboardInterrupt:
        logger.log("Keyboard interrupt detected")
        chai_db.complete_copy_expert()
        logger.log(f"Total rows processed: {total_rows}")
