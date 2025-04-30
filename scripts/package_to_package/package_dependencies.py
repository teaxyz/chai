#! /usr/bin/env pkgx +python@3.11 uv run
import argparse
import re
import sys
from typing import Any, Dict, List, Optional

from packaging import version as packaging_version
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

from core.config import Config, PackageManager
from core.db import DB
from core.logger import Logger
from core.models import DependsOn, LegacyDependency, Package, Version

# --- Constants ---
INSERT_BATCH_SIZE = 5000
DEFAULT_SEMVER_RANGE = "*"

logger = Logger("package_dependency_migration")

# --- Helper Functions ---


def preprocess_version_string(version_str: str) -> str:
    """
    Transforms known non-PEP440 version strings into a parseable format.
    Handles specific date formats, build tags, and common non-standard separators.
    """
    # === General Cleanup / Normalization ===
    # Replace underscores between digits or letters/digits
    # Digit_Digit -> Digit.Digit
    version_str = re.sub(r"(?<=\d)_(?=\d)", ".", version_str)

    # Letter_Digit -> Letter.Digit
    version_str = re.sub(r"(?<=[a-zA-Z])_(?=\d)", ".", version_str)

    # Digit_Letter -> Digit.Letter
    version_str = re.sub(r"(?<=\d)_(?=[a-zA-Z])", ".", version_str)

    # === Pattern Matching & Transformation (Order Matters!) ===

    # --- Specific Patterns First ---
    # Handle X.Y.Z-M<number> -> X.Y.Z+M<number> (Milestone)
    match_milestone = re.fullmatch(r"(\d+(\.\d+)*)-M(\d+)", version_str)
    if match_milestone:
        return f"{match_milestone.group(1)}+M{match_milestone.group(3)}"

    # Handle X.Y.Z-<string>.<number> -> X.Y.Z+<string>.<number> (Vendor Build)
    match_vendor_build = re.fullmatch(r"(\d+(\.\d+)+)-([a-zA-Z]+)\.(\d+)", version_str)
    if match_vendor_build:
        return f"{match_vendor_build.group(1)}+{match_vendor_build.group(3)}.{match_vendor_build.group(4)}"  # noqa

    # Handle X.Y.Z-git<build> -> X.Y.Z+git<build>
    match_git_build = re.fullmatch(r"(\d+(\.\d+)+)-(git[\da-zA-Z]+)", version_str)
    if match_git_build:
        return f"{match_git_build.group(1)}+{match_git_build.group(2)}"

    # Handle X.Y.Z-p<number> / X.Y.Zp<number> -> X.Y.Z+p<number>
    match_p_patch1 = re.fullmatch(r"(\d+(\.\d+)+)-p(\d+)", version_str)
    if match_p_patch1:
        return f"{match_p_patch1.group(1)}+p{match_p_patch1.group(3)}"
    match_p_patch2 = re.fullmatch(r"(\d+(\.\d+)+)p(\d+)", version_str)
    if match_p_patch2:
        return f"{match_p_patch2.group(1)}+p{match_p_patch2.group(3)}"

    # --- Date Formats ---
    # YYYY-MM-DD -> YYYY.MM.DD
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", version_str):
        return version_str.replace("-", ".")

    # YYYYMMDDTHHMMSS -> YYYYMMDD.HHMMSS
    match_ymdt_compact = re.fullmatch(r"(\d{8})T(\d{6})", version_str)
    if match_ymdt_compact:
        return f"{match_ymdt_compact.group(1)}.{match_ymdt_compact.group(2)}"

    # YYYY.MM.DD-HH.MM.SS -> YYYY.MM.DD+HHMMSS
    match_ymd_time_hyphen = re.fullmatch(
        r"(\d{4}\.\d{2}\.\d{2})-(\d{2}\.\d{2}\.\d{2})", version_str
    )
    if match_ymd_time_hyphen:
        time_part = match_ymd_time_hyphen.group(2).replace(".", "")
        return f"{match_ymd_time_hyphen.group(1)}+{time_part}"

    # ISO 8601 subset: YYYY-MM-DDTHH-MM-SSZ -> YYYY.MM.DD+HHMMSSZ
    match_iso_subset = re.fullmatch(
        r"(\d{4})-(\d{2})-(\d{2})T(\d{2})-(\d{2})-(\d{2})Z", version_str
    )
    if match_iso_subset:
        date_part = f"{match_iso_subset.group(1)}.{match_iso_subset.group(2)}.{match_iso_subset.group(3)}"  # noqa
        time_part = f"{match_iso_subset.group(4)}{match_iso_subset.group(5)}{match_iso_subset.group(6)}Z"  # noqa
        return f"{date_part}+{time_part}"

    # <datestamp>-<string|version> -> <datestamp>+<string|version>
    match_date_suffix = re.fullmatch(r"(\d{8})-?(.*)", version_str)
    if match_date_suffix and match_date_suffix.group(2):  # Ensure there is a suffix
        # Check if suffix looks like a simple version number itself,
        # otherwise treat as string
        suffix = match_date_suffix.group(2)
        # Normalize suffix by removing dots if it looks like a version part
        # This helps comparison e.g., update1 vs 3.1 -> update1 vs 31
        normalized_suffix = suffix.replace(".", "")
        return f"{match_date_suffix.group(1)}+{normalized_suffix}"

    # --- More General Build/Patch Identifiers ---
    # Handle X.Y.Z.v<build> -> X.Y.Z+v<build>
    match_v_build = re.fullmatch(r"(\d+(\.\d+)+)\.v(.*)", version_str)
    if match_v_build:
        return f"{match_v_build.group(1)}+v{match_v_build.group(3)}"

    # Handle X.Yrel.<number> -> X.Y+rel.<number>
    match_rel_build = re.fullmatch(r"(\d+(\.\d+)+)rel\.(.*)", version_str)
    if match_rel_build:
        return f"{match_rel_build.group(1)}+rel.{match_rel_build.group(3)}"

    # Handle X.Yga<number> -> X.Y+ga<number>
    match_ga_build = re.fullmatch(r"(\d+(\.\d+)+)ga(\d+)", version_str)
    if match_ga_build:
        return f"{match_ga_build.group(1)}+ga{match_ga_build.group(3)}"

    # Handle <major>-<build> (comes after more specific hyphenated patterns)
    match_major_build = re.fullmatch(r"(\d+)-([\da-zA-Z]+)", version_str)
    if match_major_build:
        return f"{match_major_build.group(1)}+{match_major_build.group(2)}"

    # Handle X.Yp<number> -> X.Y+p<number>
    match_p_patch3 = re.fullmatch(r"(\d+\.\d+)p(\d+)", version_str)
    if match_p_patch3:
        return f"{match_p_patch3.group(1)}+p{match_p_patch3.group(2)}"

    # Handle r<number> -> 0+r<number>
    match_revision = re.fullmatch(r"r(\d+)", version_str)
    if match_revision:
        return f"0+r{match_revision.group(1)}"

    # Handle X.Y<single_letter_suffix> / X.Y<two_letter_suffix> -> X.Y+suffix
    match_letter_suffix = re.fullmatch(r"(\d+\.\d+)([a-zA-Z]{1,2})", version_str)
    if match_letter_suffix:
        return f"{match_letter_suffix.group(1)}+{match_letter_suffix.group(2)}"

    # Handle leading 'p' if it looks like p<version>
    if version_str.startswith("p") and re.match(r"p\d", version_str):
        potential_version = version_str[1:]
        try:
            packaging_version.parse(potential_version)
            return potential_version
        except packaging_version.InvalidVersion:
            pass

    # --- Fallback ---
    return version_str


def get_latest_version_info(versions: List[Version]) -> Optional[Version]:
    """
    Identifies the latest version from a list using packaging.version for robust parsing
    unless there is only one version provided.

    Args:
        versions: A list of Version objects for a single package.

    Returns:
        The single Version object if only one is provided.
        Otherwise, the Version object corresponding to the latest parseable version.
        Returns None if the list is empty or no versions could be parsed (when >1 version).
    """
    # Handle empty list
    if not versions:
        return None

    # If there's only one version, return it directly without parsing
    if len(versions) == 1:
        return versions[0]

    # Proceed with parsing and comparison if more than one version exists
    latest_parsed_version = None
    latest_version_obj = None

    for version_obj in versions:
        original_version_str = version_obj.version
        preprocessed_str = preprocess_version_string(original_version_str)
        try:
            current_parsed_version = packaging_version.parse(preprocessed_str)
            if (
                latest_parsed_version is None
                or current_parsed_version > latest_parsed_version
            ):
                latest_parsed_version = current_parsed_version
                latest_version_obj = version_obj
        except packaging_version.InvalidVersion as e_invalid:
            logger.warn(
                f"'{original_version_str}' -> '{preprocessed_str}') -> {e_invalid}"
            )
            continue
        except Exception as e_general:
            logger.error(
                f"Unexpected error: '{original_version_str}' -> '{preprocessed_str}' -> {e_general}"  # noqa
            )
            continue

    # If no versions were successfully processed
    if latest_version_obj is None:
        import_id = versions[0].import_id
        versions_str = ", ".join([v.version for v in versions])
        logger.warn(f"No versions for {import_id}: {versions_str}")

    return latest_version_obj


def insert_legacy_dependencies(
    session: Session, data_batch: List[Dict[str, Any]]
) -> None:
    """
    Inserts a batch of legacy dependency records into the database,
    ignoring duplicates based on the (package_id, dependency_id) unique constraint.

    Args:
        session: The SQLAlchemy session object.
        data_batch: A list of dictionaries, each representing a LegacyDependency row.
    """
    if not data_batch:
        return

    try:
        # Get the target table object
        legacy_table = LegacyDependency.__table__

        # Construct the PostgreSQL INSERT...ON CONFLICT DO NOTHING statement
        stmt = pg_insert(legacy_table).values(data_batch)
        # Specify the columns involved in the unique constraint
        # The constraint name 'uq_package_dependency' is defined in the model
        stmt = stmt.on_conflict_do_nothing(
            index_elements=["package_id", "dependency_id"]
        )

        # Execute the statement
        session.execute(stmt)
        session.commit()

    except IntegrityError as e:
        logger.error(f"Database Integrity Error during insert: {e}")
        session.rollback()
        raise e
    except Exception as e:
        logger.error(f"An unexpected error occurred during bulk insert: {e}")
        session.rollback()
        raise e


# --- Main Processing Function ---


def process_package_dependencies(config: Config, session: Session) -> None:
    legacy_deps_to_insert: List[Dict[str, Any]] = []
    total_packages_processed = 0
    total_dependencies_found = 0
    default_dependency_type_id = config.dependency_types.runtime

    logger.log(f"Starting migration for package manager ID: {config.pm_config.pm_id}")

    # --- Fetch ALL packages for the manager ---
    logger.log("Fetching all packages for the specified manager...")
    all_packages: List[Package] = (
        session.query(Package)
        .filter(Package.package_manager_id == config.pm_config.pm_id)
        .all()  # Fetch all into memory
    )
    logger.log(f"Fetched {len(all_packages)} packages.")

    # --- Process all fetched packages ---
    for pkg in all_packages:
        total_packages_processed += 1
        if total_packages_processed % 1000 == 0:
            logger.debug(
                f"Processed {total_packages_processed}/{len(all_packages)} packages..."
            )

        versions = session.query(Version).filter(Version.package_id == pkg.id).all()
        if not versions:
            continue
        latest_version = get_latest_version_info(versions)
        if latest_version is None:
            continue

        dependencies = (
            session.query(DependsOn)
            .filter(DependsOn.version_id == latest_version.id)
            .all()
        )

        for dependency in dependencies:
            dep_data = {
                "package_id": pkg.id,
                "dependency_id": dependency.dependency_id,
                "dependency_type_id": dependency.dependency_type_id
                or default_dependency_type_id,
                "semver_range": dependency.semver_range or DEFAULT_SEMVER_RANGE,
            }
            legacy_deps_to_insert.append(dep_data)
            total_dependencies_found += 1

        # --- Check insert batch size ---
        if len(legacy_deps_to_insert) >= INSERT_BATCH_SIZE:
            logger.log(f"Reached insert batch size ({INSERT_BATCH_SIZE}). Inserting...")
            insert_legacy_dependencies(session, legacy_deps_to_insert)
            legacy_deps_to_insert = []  # Clear the batch

    # --- Final Insert ---
    if legacy_deps_to_insert:
        logger.log(
            f"Inserting final batch of {len(legacy_deps_to_insert)} dependency records."
        )
        insert_legacy_dependencies(session, legacy_deps_to_insert)

    logger.log("--- Migration Summary ---")
    logger.log(f"Total packages processed: {total_packages_processed}")
    logger.log(f"Total dependencies found: {total_dependencies_found}")
    logger.log("Migration process completed.")


# --- Main Execution ---

if __name__ == "__main__":
    desc = """Migrate version-specific dependencies to package-level dependencies based 
    on the latest version."""
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument(
        "--package-manager",
        type=lambda pm: PackageManager[pm.upper()],
        choices=list(PackageManager),
        required=True,
        help="The package manager to process (e.g., NPM, CRATES).",
    )

    args = parser.parse_args()

    logger.log(
        f"Starting package dependency migration for: {args.package_manager.name}"
    )

    SessionLocal = None
    try:
        config = Config(args.package_manager)
        db = DB("db_logger")
        SessionLocal = sessionmaker(bind=db.engine)

        with SessionLocal() as session:
            process_package_dependencies(config, session)

    except Exception as e:
        logger.error(f"An critical error occurred: {e}")
        sys.exit(1)
    finally:
        logger.log("Script finished.")
