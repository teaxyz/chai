#!/usr/bin/env pkgx +python@3.11 uv run --with psycopg2==2.9.9

import argparse
import csv
import os
import uuid
from datetime import datetime
from typing import List, Optional, Set, Tuple

import psycopg2
import psycopg2.extras

from core.config import Config, PackageManager
from core.logger import Logger

CHAI_DATABASE_URL = os.environ.get("CHAI_DATABASE_URL")
DEFAULT_BATCH_SIZE = 20000
OUTPUT_CSV_FILENAME = "inserted_urls.csv"


class ChaiDB:
    """Handles interactions with the CHAI database for batch URL insertion."""

    def __init__(self):
        """Initialize connection to the CHAI database."""
        self.logger = Logger("batch_url_db")
        if not CHAI_DATABASE_URL:
            self.logger.error("CHAI_DATABASE_URL environment variable not set.")
            raise ValueError("CHAI_DATABASE_URL not set")
        self.conn = None
        self.cursor = None
        try:
            self.conn = psycopg2.connect(CHAI_DATABASE_URL)
            self.cursor = self.conn.cursor()
            self.logger.log("CHAI database connection established")
        except psycopg2.Error as e:
            self.logger.error(f"Database connection error: {e}")
            raise

    def batch_insert_urls(
        self,
        url_data_tuples: List[Tuple[str, uuid.UUID, datetime, datetime]],
        dump_output: bool,
    ) -> Optional[List[Tuple[uuid.UUID, str, uuid.UUID]]]:
        """
        Batch insert URLs into the database.

        Args:
            url_data_tuples: A list of tuples, each containing
                             (url, url_type_id, created_at_ts, updated_at_ts).
            dump_output: If True, return the inserted/updated rows.

        Returns:
            A list of (id, url, url_type_id) tuples if dump_output is True, else None.
        """
        if not url_data_tuples:
            return [] if dump_output else None

        query_base = """
            INSERT INTO urls (url, url_type_id, created_at, updated_at)
            VALUES %s
            ON CONFLICT (url_type_id, url) DO UPDATE SET updated_at = EXCLUDED.updated_at
        """
        if dump_output:
            query = query_base + " RETURNING id, url, url_type_id"
        else:
            query = query_base

        try:
            psycopg2.extras.execute_values(
                self.cursor, query, url_data_tuples, page_size=len(url_data_tuples)
            )
            self.conn.commit()
            self.logger.log(
                f"Successfully inserted/updated {len(url_data_tuples)} URL records."
            )
            if dump_output:
                return self.cursor.fetchall()
            return None
        except psycopg2.Error as e:
            self.logger.error(f"Error during batch insert: {e}")
            self.logger.log(url_data_tuples)
            self.conn.rollback()
            raise e
        except Exception as e:
            self.logger.error(f"An unexpected error occurred during batch insert: {e}")
            self.conn.rollback()
            raise e

    def close(self):
        """Close the database connection."""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        self.logger.log("CHAI database connection closed")


def process_urls_for_batch_insert(
    file_path: str,
    batch_size: int,
    script_execution_time: datetime,
    dump_output: bool,
    stop_at: Optional[int] = None,
) -> None:
    """
    Reads URLs from a CSV file, prepares them, and batch inserts them into the database.

    Args:
        file_path: Path to the input CSV file.
        batch_size: Number of records to insert per batch.
        script_execution_time: Timestamp for created_at/updated_at.
        dump_output: Whether to dump inserted data to a CSV file.
        stop_at: Optional number of CSV rows to process.
    """
    logger = Logger("url_batch_processor")
    logger.log(f"Starting URL batch processing for file: {file_path}")
    logger.log(
        f"Batch size: {batch_size}, Dump output: {dump_output}, Stop at: {stop_at}"
    )
    cache: Set[Tuple[str, uuid.UUID]] = set()

    try:
        config = Config(PackageManager.NPM)
        url_type_homepage_id = config.url_types.homepage
        url_type_source_id = config.url_types.source
    except AttributeError as e:
        logger.error(
            f"Could not load URL types from config. Ensure DB contains these types: {e}"
        )
        return
    except Exception as e:
        logger.error(f"Error initializing config: {e}")
        return

    chai_db = None
    try:
        chai_db = ChaiDB()
    except Exception as e:
        logger.error(f"Failed to initialize ChaiDB: {e}")
        return  # Exit if DB connection fails

    url_data_to_insert: List[Tuple[str, uuid.UUID, datetime, datetime]] = []
    all_inserted_data_for_dump: List[Tuple[uuid.UUID, str, uuid.UUID]] = []
    processed_csv_rows = 0
    total_urls_prepared = 0

    try:
        with open(file_path, "r", newline="", encoding="utf-8") as csvfile:
            reader = csv.reader(csvfile)
            header = next(reader, None)  # Skip header
            if not header:
                logger.warn("CSV file is empty or has no header.")
                return

            logger.log(f"CSV Header: {header}")  # Log the header for context

            for row in reader:
                processed_csv_rows += 1
                if not (len(row) >= 3):
                    logger.warn(f">3 cols at L{processed_csv_rows + 1}: {row}")
                    continue

                # Assuming import_id is row[0], source is row[1], homepage is row[2]
                # set the source data
                source_url = row[1].strip() if row[1] else None
                source_data = (source_url, url_type_source_id)

                # set the homepage data
                homepage_url = row[2].strip() if row[2] else None
                homepage_data = (homepage_url, url_type_homepage_id)

                # add to url_data_to_insert if valid and not in cache
                # also, update the cache
                urls_to_process = []
                if (
                    source_url
                    and source_url.lower() != "null"
                    and source_data not in cache
                ):
                    urls_to_process.append(source_data)
                    cache.add(source_data)
                if (
                    homepage_url
                    and homepage_url.lower() != "null"
                    and homepage_data not in cache
                ):
                    urls_to_process.append(homepage_data)
                    cache.add(homepage_data)

                for url_str, url_type_id in urls_to_process:
                    url_data_to_insert.append(
                        (
                            url_str,
                            url_type_id,
                            script_execution_time,
                            script_execution_time,
                        )
                    )
                    total_urls_prepared += 1

                # insert the data in batches
                if len(url_data_to_insert) >= batch_size:
                    results = chai_db.batch_insert_urls(url_data_to_insert, dump_output)
                    if dump_output and results:
                        all_inserted_data_for_dump.extend(results)
                    url_data_to_insert = []
                    logger.log(
                        f"Processed batch. Total CSV rows read: {processed_csv_rows}, Total URLs prepared: {total_urls_prepared}"  # noqa
                    )

                if stop_at and processed_csv_rows >= stop_at:
                    logger.log(f"Reached stop limit of {stop_at} CSV rows.")
                    break

        # Process any remaining URLs in the buffer
        if url_data_to_insert:
            results = chai_db.batch_insert_urls(url_data_to_insert, dump_output)
            if dump_output and results:
                all_inserted_data_for_dump.extend(results)
            logger.log(
                f"Processed final batch. Total CSV rows read: {processed_csv_rows}, Total URLs prepared: {total_urls_prepared}"  # noqa
            )

        if dump_output:
            with open(
                OUTPUT_CSV_FILENAME, "w", newline="", encoding="utf-8"
            ) as outfile:
                writer = csv.writer(outfile)
                writer.writerow(["id", "url", "url_type_id"])  # Header for output CSV
                writer.writerows(all_inserted_data_for_dump)
            logger.log(
                f"Dumped {len(all_inserted_data_for_dump)} records to {OUTPUT_CSV_FILENAME}"  # noqa
            )

        logger.log(
            f"URL batch processing complete. Total CSV rows processed: {processed_csv_rows}. Total URLs prepared/processed: {total_urls_prepared}."  # noqa
        )

    except FileNotFoundError:
        logger.error(f"Input CSV file not found: {file_path}")
    except csv.Error as e:
        logger.error(
            f"CSV reading error in {file_path} near line {reader.line_num}: {e}"
        )
    except psycopg2.Error as e:
        logger.error(f"A database error occurred: {e}")
        logger.exception()
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        logger.exception()
    finally:
        if chai_db:
            chai_db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Batch insert URLs from a CSV file into the CHAI database."
    )
    parser.add_argument("file_path", help="Path to the input CSV file (e.g., data.csv)")
    parser.add_argument(
        "--batch-size",
        "-b",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f"Number of records to insert per batch (default: {DEFAULT_BATCH_SIZE})",
    )
    parser.add_argument(
        "--stop",
        "-s",
        type=int,
        help="Optional: stop processing after this many CSV rows.",
    )
    parser.add_argument(
        "--dump-output",
        "-d",
        action="store_true",
        help=f"If set, dump all inserted/updated (id, url, url_type_id) to {OUTPUT_CSV_FILENAME}",
    )

    args = parser.parse_args()

    script_start_time = datetime.now()
    main_logger = Logger("main_batch_url_loader")
    main_logger.log(f"Script started at {script_start_time.isoformat()}")

    process_urls_for_batch_insert(
        file_path=args.file_path,
        batch_size=args.batch_size,
        script_execution_time=script_start_time,  # Use a consistent time for the whole run
        dump_output=args.dump_output,
        stop_at=args.stop,
    )

    main_logger.log(
        f"Script finished. Total execution time: {datetime.now() - script_start_time}"
    )
