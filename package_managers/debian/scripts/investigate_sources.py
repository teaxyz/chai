#!/usr/bin/env pkgx uv run

"""
Script to investigate the relationship between Debian sources and packages files.
This helps understand the data structure before modifying the parser.
"""

import os
import sys

from core.logger import Logger

logger = Logger("debian_investigator")


def parse_sources_file(file_path: str) -> dict[str, set[str]]:
    """
    Parse the sources file and return a mapping of source_name -> set of binary packages.

    Args:
        file_path: Path to the sources file

    Returns:
        Dictionary mapping source package names to sets of binary package names they produce
    """
    source_binary_map = {}

    with open(file_path, encoding="utf-8") as f:
        current_package = None
        current_binaries = set()
        in_binary_field = False

        for line in f:
            original_line = line
            line = line.strip()

            if line.startswith("Package: "):
                # Save previous package if exists
                if current_package:
                    if current_package in source_binary_map:
                        # Merge with existing binaries for this source name
                        source_binary_map[current_package].update(current_binaries)
                    else:
                        source_binary_map[current_package] = current_binaries

                # Start new package
                current_package = line[9:].strip()
                current_binaries = set()
                in_binary_field = False

            elif line.startswith("Binary: "):
                # Parse binary packages (comma-separated, may continue on next lines)
                binaries_str = line[8:].strip()
                binaries = [b.strip() for b in binaries_str.split(",") if b.strip()]
                current_binaries.update(binaries)
                in_binary_field = True

            elif current_package and original_line.startswith(" "):
                # Continuation line (starts with space)
                if in_binary_field:
                    # Continue parsing Binary field
                    binaries_str = line.strip()
                    binaries = [b.strip() for b in binaries_str.split(",") if b.strip()]
                    current_binaries.update(binaries)
                # If not in binary field, it's some other field continuation - ignore

            elif line == "" and current_package:
                # End of current package entry
                if current_package in source_binary_map:
                    # Merge with existing binaries for this source name
                    source_binary_map[current_package].update(current_binaries)
                else:
                    source_binary_map[current_package] = current_binaries
                current_package = None
                current_binaries = set()
                in_binary_field = False

            else:
                # Any other field (not Package, not Binary, not continuation)
                # This includes new fields that don't start with space
                in_binary_field = False

        # Handle last package if file doesn't end with blank line
        if current_package:
            if current_package in source_binary_map:
                # Merge with existing binaries for this source name
                source_binary_map[current_package].update(current_binaries)
            else:
                source_binary_map[current_package] = current_binaries

    return source_binary_map


def parse_packages_file(file_path: str) -> dict[str, str | None]:
    """
    Parse the packages file and return a mapping of package_name -> source_name.

    Args:
        file_path: Path to the packages file

    Returns:
        Dictionary mapping package names to their source package names (None if not specified)
    """
    package_source_map = {}

    with open(file_path, encoding="utf-8") as f:
        current_package = None
        current_source = None

        for line in f:
            line = line.strip()

            if line.startswith("Package: "):
                # Save previous package if exists
                if current_package:
                    package_source_map[current_package] = current_source

                # Start new package
                current_package = line[9:].strip()
                current_source = None

            elif line.startswith("Source: "):
                # Extract source name (may include version info in parentheses)
                source_str = line[8:].strip()
                # Remove version info if present: "source (version)" -> "source"
                if "(" in source_str:
                    current_source = source_str.split("(")[0].strip()
                else:
                    current_source = source_str

            elif line == "" and current_package:
                # End of current package entry
                package_source_map[current_package] = current_source
                current_package = None
                current_source = None

        # Handle last package if file doesn't end with blank line
        if current_package:
            package_source_map[current_package] = current_source

    return package_source_map


def investigate_mapping(sources_file: str, packages_file: str) -> None:
    """
    Investigate the mapping between sources and packages files.

    Args:
        sources_file: Path to the sources file
        packages_file: Path to the packages file
    """
    logger.log("Parsing sources file...")
    source_binary_map = parse_sources_file(sources_file)
    logger.log(f"Found {len(source_binary_map)} source packages")

    logger.log("Parsing packages file...")
    package_source_map = parse_packages_file(packages_file)
    logger.log(f"Found {len(package_source_map)} binary packages")

    # Validate mappings
    orphaned_packages = []

    logger.log("\nValidating package -> source mappings...")

    for package_name, source_name in package_source_map.items():
        if source_name:
            # Package has explicit source reference
            if source_name not in source_binary_map:
                logger.log(
                    f"WARNING: Package '{package_name}' references unknown source '{source_name}'"
                )
                orphaned_packages.append((package_name, source_name, "unknown_source"))
            elif package_name not in source_binary_map[source_name]:
                logger.log(
                    f"WARNING: Package '{package_name}' not listed in source '{source_name}' binaries"
                )
                orphaned_packages.append((package_name, source_name, "not_in_binaries"))
        else:
            # Package has no explicit source, assume source name == package name
            if package_name not in source_binary_map:
                logger.log(
                    f"WARNING: Package '{package_name}' has no source reference and no matching source package"
                )
                orphaned_packages.append(
                    (package_name, package_name, "no_matching_source")
                )
            elif package_name not in source_binary_map[package_name]:
                logger.log(
                    f"WARNING: Package '{package_name}' not listed in its own source binaries"
                )
                orphaned_packages.append(
                    (package_name, package_name, "not_self_listed")
                )

    # Summary
    logger.log("\n=== SUMMARY ===")
    logger.log(f"Total sources: {len(source_binary_map)}")
    logger.log(f"Total packages: {len(package_source_map)}")
    logger.log(f"Orphaned packages: {len(orphaned_packages)}")

    if orphaned_packages:
        logger.log("\nOrphaned packages by category:")
        categories = {}
        for pkg, src, reason in orphaned_packages:
            if reason not in categories:
                categories[reason] = []
            categories[reason].append((pkg, src))

        for reason, items in categories.items():
            logger.log(f"  {reason}: {len(items)} packages")
            for pkg, src in items[:5]:  # Show first 5 examples
                logger.log(f"    {pkg} -> {src}")
            if len(items) > 5:
                logger.log(f"    ... and {len(items) - 5} more")


def main():
    data_dir = "data/debian/latest"

    # Check if data files exist
    sources_file = os.path.join(data_dir, "sources")
    packages_file = os.path.join(data_dir, "packages")

    if not os.path.exists(sources_file):
        logger.log(f"ERROR: Sources file not found at {sources_file}")
        logger.log("Use --fetch to download the latest data")
        return 1

    if not os.path.exists(packages_file):
        logger.log(f"ERROR: Packages file not found at {packages_file}")
        logger.log("Use --fetch to download the latest data")
        return 1

    logger.log(f"Using sources file: {sources_file}")
    logger.log(f"Using packages file: {packages_file}")

    investigate_mapping(sources_file, packages_file)

    return 0


if __name__ == "__main__":
    sys.exit(main())
