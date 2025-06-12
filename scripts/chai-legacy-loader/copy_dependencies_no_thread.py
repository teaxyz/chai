#!/usr/bin/env pkgx +python@3.11 uv run
import argparse
import io
import os
import uuid

import psycopg2
import psycopg2.errors

from core.config import Config, PackageManager
from core.logger import Logger

LEGACY_CHAI_DATABASE_URL = os.environ.get("LEGACY_CHAI_DATABASE_URL")
CHAI_DATABASE_URL = os.environ.get("CHAI_DATABASE_URL")
BATCH_SIZE = 20000
LEGACY_CHAI_PACKAGE_MANAGER_MAP: dict[PackageManager, str] = {
    PackageManager.NPM: "npm",
    PackageManager.CRATES: "crates",
    PackageManager.HOMEBREW: "brew",
    PackageManager.DEBIAN: "apt",
    PackageManager.PKGX: "pkgx",
}


class LegacyDB:
    """Handles all interactions with the legacy CHAI database."""

    def __init__(self, input_package_manager: PackageManager):
        """Initialize connection to the legacy database."""
        self.conn = psycopg2.connect(LEGACY_CHAI_DATABASE_URL)
        # Set autocommit to False for server-side cursors
        self.conn.set_session(autocommit=False)
        self.logger = Logger("legacy_db")
        self.logger.debug("Legacy database connection established")
        self.package_manager_name = LEGACY_CHAI_PACKAGE_MANAGER_MAP[
            input_package_manager
        ]

    def __del__(self):
        """Close connection when object is destroyed."""
        if hasattr(self, "conn") and self.conn:
            self.conn.close()

    def get_sql_content(self, filename: str) -> str:
        """Load SQL content from a file."""
        sql_file_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "sql", filename
        )
        with open(sql_file_path) as f:
            return f.read()

    def create_server_cursor(self, sql_file: str, cursor_name: str) -> None:
        """Create a server-side cursor for efficient data fetching.

        Inputs:
            sql_file: The name of the SQL file to load
            cursor_name: The name of the cursor to create
            package_manager_name: The name of the package manager whose legacy data we
                are fetching
        """
        query = self.get_sql_content(sql_file)

        # substitute $1 with self.package_manager_name
        query = query.replace("$1", f"'{self.package_manager_name}'")
        self.logger.debug(f"Query: {query}")

        # create a named server side cursor for retrieving data
        declare_stmt = f"DECLARE {cursor_name} CURSOR FOR {query}"

        # create a cursor to execute the declare statement
        with self.conn.cursor() as cursor:
            cursor.execute(declare_stmt)
            self.logger.debug(
                f"Created server-side cursor '{cursor_name}' for {sql_file}"
            )

    def fetch_batch(self, cursor_name: str, batch_size: int) -> list[tuple]:
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

    def __init__(self, config: Config):
        """Initialize connection to the CHAI database."""
        self.logger = Logger("chai_db")
        self.config = config

        # connect to the database
        self.conn = psycopg2.connect(CHAI_DATABASE_URL)
        # Use autocommit=False for server-side cursors if needed within a transaction
        # self.conn.set_session(autocommit=False)
        self.logger.debug("CHAI database connection established")

        # create the cursor for general operations
        self.cursor = self.conn.cursor()
        self.logger.debug("CHAI database cursor created")

        # configure some variables
        self.legacy_dependency_columns = [
            "package_id",
            "dependency_id",
            # the below two are not available from the sources table in the legacy db
            # assuming everything is a runtime dependency and use the semver range *
            "dependency_type_id",
            "semver_range",
        ]
        # initialize package map
        self.package_map = self._get_package_map()
        self.logger.debug(
            f"{len(self.package_map)} {self.config.pm_config.package_manager} packages in CHAI"
        )

        # Load existing legacy dependencies to avoid duplicates
        self.processed_pairs = set()
        self._load_existing_dependencies()

    def _get_package_map(self) -> dict[str, uuid.UUID]:
        """Get a map of package import_ids to their UUIDs for the configured package
        manager"""
        query = """SELECT import_id, id 
            FROM packages 
            WHERE package_manager_id = %(pm_id)s
            AND import_id IS NOT NULL"""
        self.cursor.execute(query, {"pm_id": self.config.pm_config.pm_id})
        rows = self.cursor.fetchall()

        # check that we actually loaded packages for the specified manager
        if len(rows) == 0:
            raise ValueError(
                f"{self.config.pm_config.package_manager} packages not found in DB"
            )

        return {row[0]: row[1] for row in rows}

    def _load_existing_dependencies(self, batch_size: int = BATCH_SIZE) -> None:
        """
        Loads existing (package_id, dependency_id) pairs from the
        legacy_dependencies table into self.processed_pairs using a
        server-side cursor to handle potentially large datasets efficiently.
        """
        self.logger.log("Loading existing legacy dependencies...")
        query = "SELECT package_id, dependency_id FROM legacy_dependencies"
        cursor_name = "existing_deps_cursor"
        total_loaded = 0

        # Use a transaction context for the server-side cursor
        with self.conn, self.conn.cursor(name=cursor_name) as named_cursor:
            named_cursor.execute(query)
            while True:
                batch = named_cursor.fetchmany(batch_size)
                if not batch:
                    break
                # Convert batch of tuples to set for efficient update
                self.processed_pairs.update(batch)
                total_loaded += len(batch)
                if total_loaded % (batch_size * 20000) == 0:
                    self.logger.debug(
                        f"Loaded {total_loaded} existing dependency pairs..."
                    )

        self.logger.log(
            f"Finished loading {total_loaded} existing dependency pairs into memory."
        )

    def init_copy_expert(self) -> None:
        """Initialize a StringIO object to collect CSV data for copy operation"""
        self.csv_data = io.StringIO()
        self.columns_str = ", ".join(self.legacy_dependency_columns)
        self.logger.debug("Copy buffer initialized")

    def add_rows_to_copy_expert(self, rows: list[tuple]) -> int:
        """Add rows to the StringIO buffer for later COPY operation"""
        rows_added = 0
        for row in rows:
            package_id = self.package_map.get(row[0])
            dependency_id = self.package_map.get(row[1])

            # if package or dependency are not found, skip the row
            if not package_id or not dependency_id:
                # skipping because maybe the package or dependency is
                #  not in legacy chai
                #  marked as spam
                continue

            # if the pair has already been processed, skip the row
            if (package_id, dependency_id) in self.processed_pairs:
                continue

            # add the pair to the processed pairs
            self.processed_pairs.add((package_id, dependency_id))

            # get the dependency type and semver range
            # not available from the sources table in the legacy db
            # assume everything is a runtime dependency, and use the semver range *
            dependency_type_id = self.config.dependency_types.runtime
            semver_range = "*"

            csv_line = (
                f"{package_id},{dependency_id},{dependency_type_id},{semver_range}"
            )
            self.csv_data.write(csv_line + "\n")
            rows_added += 1

        return rows_added

    def add_rows_with_flush(self, rows: list[tuple], max_buffer_size=100000) -> int:
        """Add rows to the StringIO buffer for later COPY operation"""
        rows_added = self.add_rows_to_copy_expert(rows)
        self.logger.log(f"Added {rows_added} rows to the copy expert")

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
            self.logger.log(f"{len(self.processed_pairs)} total rows copied")
        except psycopg2.errors.BadCopyFileFormat as e:
            self.logger.log(f"Error copying data to database: {e}")
            # write the csv data to a file
            with open("bad_copy_file.csv", "w") as f:
                f.write(self.csv_data.getvalue())
            self.conn.rollback()
            raise e


def main(
    logger: Logger,
    config: Config,
    input_package_manager: PackageManager,
    stop: int | None,
) -> None:
    legacy_db = LegacyDB(input_package_manager)
    chai_db = ChaiDB(config)

    # initialize the copy expert
    chai_db.init_copy_expert()

    # set up the legacy db
    cursor_name = "legacy_dependencies_cursor"
    legacy_db.create_server_cursor("dependencies.sql", cursor_name)

    logger.log("Starting dependency loop process")
    total_rows = 0
    try:
        while True:
            rows = legacy_db.fetch_batch(cursor_name, BATCH_SIZE)

            # break if we have no more rows
            if not rows:
                break

            # keep adding the rows to the copy expert
            rows_added = chai_db.add_rows_with_flush(rows)

            # update the total rows processed
            total_rows += rows_added

            # break if we have processed the stop number of rows
            if stop and total_rows >= stop:
                break

        # complete the copy expert
        logger.log("Completing copy expert for the last batch")
        chai_db.complete_copy_expert()

    except KeyboardInterrupt:
        logger.log("Keyboard interrupt detected")
        chai_db.complete_copy_expert()
        logger.log(f"Total rows processed: {total_rows}")

    finally:
        logger.log(f"Total rows processed: {total_rows}")
        legacy_db.close_cursor(cursor_name)
        legacy_db.conn.close()
        chai_db.cursor.close()
        chai_db.conn.close()
        logger.log("Database connections closed")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--package-manager",
        type=PackageManager,
        choices=list(PackageManager),
        required=True,
    )
    parser.add_argument(
        "--stop",
        type=int,
        default=None,
        help="Stop after processing a certain number of rows",
    )
    args = parser.parse_args()

    input_package_manager: PackageManager = args.package_manager
    stop: int | None = args.stop
    logger = Logger("chai_legacy_loader")
    config = Config(input_package_manager)

    logger.log(f"Importing legacy dependencies for {args.package_manager}")
    main(
        logger,
        config,
        input_package_manager,
        stop,
    )
