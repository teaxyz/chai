#!/usr/bin/env pkgx uv run

import argparse
import csv
import sys
from uuid import UUID

from scripts.upgrade_canons.db import DB


def read_package_data_from_csv(filename: str) -> list[tuple[str, UUID]]:
    """Read package names and canon IDs from CSV file and return as list of tuples."""
    package_data = []
    try:
        with open(filename, newline="") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                try:
                    package_name = row["package_name"]
                    canon_id = UUID(row["canon_id"])
                    package_data.append((package_name, canon_id))
                except (ValueError, KeyError) as e:
                    print(
                        f"Warning: Invalid row in CSV '{row}': {e}",
                        file=sys.stderr,
                    )
    except FileNotFoundError:
        print(f"Error: File '{filename}' not found", file=sys.stderr)
        sys.exit(1)
    return package_data


def process_deleted_package(
    db: DB, package_name: str, target_canon_id: UUID, dry_run: bool
) -> tuple[bool, str]:
    """
    Process a single package name for deleted registered projects.
    Returns (success, reason) tuple.
    """
    # Step 1: Prepend 'npm/' to the name to create derived_id
    derived_id = f"npm/{package_name}"

    # Step 2: Search by derived_id to get the package_id
    db.cursor.execute(
        """
        SELECT id 
        FROM packages 
        WHERE derived_id = %s
    """,
        (derived_id,),
    )

    package_result = db.cursor.fetchone()
    if not package_result:
        return False, "could not find derived_id"

    package_id = package_result[0]

    # Step 3: Join to canon_packages to retrieve the current canon_id
    db.cursor.execute(
        """
        SELECT canon_id 
        FROM canon_packages 
        WHERE package_id = %s
    """,
        (package_id,),
    )

    current_result = db.cursor.fetchone()
    if not current_result:
        return False, "could not find current canon_id"

    current_canon_id = current_result[0]

    if dry_run:
        print(
            f"DRY RUN: Would update canon_id {current_canon_id} to {target_canon_id} for package {derived_id} (package_id: {package_id})"
        )
        return True, ""

    try:
        # Run the three update statements using target_canon_id from CSV
        # 1. Update canons table
        db.cursor.execute(
            """
            UPDATE canons
            SET id = %s
            WHERE id = %s
        """,
            (target_canon_id, current_canon_id),
        )

        # 2. Update canon_packages table
        db.cursor.execute(
            """
            UPDATE canon_packages
            SET canon_id = %s
            WHERE canon_id = %s
        """,
            (target_canon_id, current_canon_id),
        )

        # 3. Update tea_ranks table
        db.cursor.execute(
            """
            UPDATE tea_ranks
            SET canon_id = %s
            WHERE canon_id = %s
        """,
            (target_canon_id, current_canon_id),
        )

        return True, ""

    except Exception as e:
        print(
            f"Error updating canon_id for package {package_name}: {e}", file=sys.stderr
        )
        return False, f"database error: {e!s}"


def write_failures_csv(
    failures: list[tuple[str, str]], filename: str = "deleted_canons_failures.csv"
):
    """Write failures to a CSV file."""
    with open(filename, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["package_name", "reason"])
        for package_name, reason in failures:
            writer.writerow([package_name, reason])


def main():
    parser = argparse.ArgumentParser(
        description="Create canons for registered projects that were deleted"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without making changes",
    )
    parser.add_argument(
        "--csv-file",
        type=str,
        required=True,
        help="CSV file containing package_name and canon_id columns",
    )
    args = parser.parse_args()

    # Read package data from CSV
    package_data = read_package_data_from_csv(args.csv_file)

    if not package_data:
        print("No package data provided", file=sys.stderr)
        sys.exit(1)

    print(
        f"Processing {len(package_data)} package records for deleted registered projects..."
    )

    # Initialize database connection
    db = DB()

    success_count = 0
    failure_count = 0
    failures = []

    try:
        for package_name, target_canon_id in package_data:
            success, reason = process_deleted_package(
                db, package_name, target_canon_id, args.dry_run
            )

            if success:
                success_count += 1
            else:
                failure_count += 1
                failures.append((package_name, reason))
                print(f"Warning: Failed to process package {package_name}: {reason}")

        # Commit changes if not dry run
        if not args.dry_run and success_count > 0:
            db.conn.commit()
            print("Database changes committed.")

        # Write failures to CSV if any
        if failures:
            write_failures_csv(failures)
            print("Failures written to deleted_canons_failures.csv")

    finally:
        db.close()

    # Print final summary
    print("--------------------------------------------------")
    print(f"✅ Success: {success_count}")
    print(f"❌ Failure: {failure_count}")
    print("--------------------------------------------------")


if __name__ == "__main__":
    main()
