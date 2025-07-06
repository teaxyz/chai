#!/usr/bin/env pkgx +python@3.11 uv run

"""
For a csv generated from legacy chai, this script adds the id, created_at, and
updated_at fields to the csv.

The input CSV must have a header row: "derived_id,name,import_id".
The package_manager argument must be a valid UUID.

Usage:
    chmod +x add-package-fields.py
    ./add-package-fields.py input.csv output.csv <package_manager_uuid>
"""

import csv
import sys
import uuid
from datetime import UTC, datetime


def validate_uuid(uuid_string: str) -> None:
    """Raises ValueError if the string is not a valid UUID."""
    try:
        uuid.UUID(uuid_string)
    except ValueError as exc:
        raise ValueError(f"Invalid UUID format: {uuid_string}") from exc


def process_csv(input_file: str, output_file: str, package_manager_id: str) -> None:
    """
    Processes the input CSV, validates headers, adds new fields, and writes to the
    output CSV.

    Args:
        input_file: Path to the input CSV file.
        output_file: Path to the output CSV file.
        package_manager_id: The UUID of the package manager.

    Raises:
        ValueError: If the input CSV header is missing or incorrect.
    """
    now = datetime.now(UTC).isoformat()
    expected_header: list[str] = ["derived_id", "name", "import_id"]
    output_header: list[str] = [
        "id",
        "derived_id",
        "name",
        "package_manager_id",
        "import_id",
        "created_at",
        "updated_at",
    ]

    with (
        open(input_file, newline="") as infile,
        open(output_file, "w", newline="") as outfile,
    ):
        reader: csv._reader = csv.reader(infile)
        writer: csv._writer = csv.writer(outfile)

        # 1. Validate header row
        header: list[str] | None = next(reader, None)
        if header is None:
            raise ValueError(f"Input file '{input_file}' is missing a header row.")
        if header != expected_header:
            raise ValueError(
                f"Input file '{input_file}' header mismatch. "
                f"Expected: {expected_header}, Got: {header}"
            )

        # Write output header
        writer.writerow(output_header)

        # Process data rows
        row_count = 0
        for row in reader:
            if len(row) != len(expected_header):
                msg = f"Warning: Skipping row {reader.line_num} due to incorrect \
                    column count ({len(row)} instead of {len(expected_header)}): {row}"
                print(msg, file=sys.stderr)
                continue

            row_uuid: str = str(uuid.uuid4())
            derived_id, name, import_id = row
            output_row: list[str] = [
                row_uuid,
                derived_id,
                name,
                package_manager_id,
                import_id,
                now,
                now,
            ]
            writer.writerow(output_row)
            row_count += 1

    print(f"Processed {row_count} rows from {input_file} -> {output_file}")


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print(
            f"Usage: {sys.argv[0]} input.csv output.csv <package_manager_uuid>",
            file=sys.stderr,
        )
        sys.exit(1)

    input_csv_path: str = sys.argv[1]
    output_csv_path: str = sys.argv[2]
    pm_uuid: str = sys.argv[3]

    try:
        # 6. Validate package_manager argument is a UUID
        validate_uuid(pm_uuid)
        process_csv(input_csv_path, output_csv_path, pm_uuid)
    except FileNotFoundError as e:
        print(f"Error: Input file not found - {e}", file=sys.stderr)
        sys.exit(1)
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"An unexpected error occurred: {e}", file=sys.stderr)
        sys.exit(1)
