#!/usr/bin/env pkgx uv run

import argparse
import csv
import sys
from uuid import UUID

from scripts.upgrade_canons.db import DB


def read_canon_ids_from_stdin() -> list[UUID]:
    """Read canon IDs from stdin and return as list of UUIDs."""
    canon_ids = []
    for line in sys.stdin:
        line = line.strip()
        if line:
            try:
                canon_ids.append(UUID(line))
            except ValueError as e:
                print(f"Warning: Invalid UUID format '{line}': {e}", file=sys.stderr)
    return canon_ids


def process_canon_id(db: DB, canon_id: UUID, dry_run: bool) -> tuple[bool, str]:
    """
    Process a single canon ID and perform the updates.
    Returns (success, reason) tuple.
    """
    # First, join to canon_packages_old to get package_id
    db.cursor.execute(
        """
        SELECT package_id 
        FROM canon_packages_old 
        WHERE canon_id = %s
    """,
        (canon_id,),
    )

    old_result = db.cursor.fetchone()
    if not old_result:
        return False, "could not find package_id"

    package_id = old_result[0]

    # Next, join to canon_packages to get current canon_id
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
        return False, "could not find new canon_id"

    new_canon_id = current_result[0]

    if dry_run:
        print(
            f"DRY RUN: Would update canon_id {new_canon_id} to {canon_id} for package {package_id}"
        )
        return True, ""

    try:
        # Run the three update statements
        # 1. Update canons table
        db.cursor.execute(
            """
            UPDATE canons
            SET id = %s
            WHERE id = %s
        """,
            (canon_id, new_canon_id),
        )

        # 2. Update canon_packages table
        db.cursor.execute(
            """
            UPDATE canon_packages
            SET canon_id = %s
            WHERE canon_id = %s
        """,
            (canon_id, new_canon_id),
        )

        # 3. Update tea_ranks table
        db.cursor.execute(
            """
            UPDATE tea_ranks
            SET canon_id = %s
            WHERE canon_id = %s
        """,
            (canon_id, new_canon_id),
        )

        return True, ""

    except Exception as e:
        print(f"Error updating canon_id {canon_id}: {e}", file=sys.stderr)
        return False, f"database error: {e!s}"


def write_failures_csv(
    failures: list[tuple[UUID, str]], filename: str = "canon_update_failures.csv"
):
    """Write failures to a CSV file."""
    with open(filename, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["canon_id", "reason"])
        for canon_id, reason in failures:
            writer.writerow([str(canon_id), reason])


def main():
    parser = argparse.ArgumentParser(
        description="Update Canon IDs for registered projects"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without making changes",
    )
    args = parser.parse_args()

    # Read canon IDs from stdin
    canon_ids = read_canon_ids_from_stdin()

    if not canon_ids:
        print("No canon IDs provided via stdin", file=sys.stderr)
        sys.exit(1)

    print(f"Processing {len(canon_ids)} canon IDs...")

    # Initialize database connection
    db = DB()

    success_count = 0
    failure_count = 0
    failures = []

    try:
        for canon_id in canon_ids:
            success, reason = process_canon_id(db, canon_id, args.dry_run)

            if success:
                success_count += 1
            else:
                failure_count += 1
                failures.append((canon_id, reason))
                print(f"Warning: Failed to process canon_id {canon_id}: {reason}")

        # Commit changes if not dry run
        if not args.dry_run and success_count > 0:
            db.conn.commit()
            print("Database changes committed.")

        # Write failures to CSV if any
        if failures:
            write_failures_csv(failures)
            print("Failures written to canon_update_failures.csv")

    finally:
        db.close()

    # Print final summary
    print("--------------------------------------------------")
    print(f"✅ Success: {success_count}")
    print(f"❌ Failure: {failure_count}")
    print("--------------------------------------------------")


if __name__ == "__main__":
    main()
