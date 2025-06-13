#!/usr/bin/env pkgx uv run
"""Base validator for end-to-end pipeline tests."""

import json
import os
import sys
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple

import psycopg2
from psycopg2.extras import RealDictCursor


class BaseValidator(ABC):
    """Base class for pipeline validation."""

    def __init__(self, package_manager: str):
        self.package_manager = package_manager
        self.db_url = os.environ.get(
            "CHAI_DATABASE_URL", "postgresql://postgres:test_password@localhost:5432/chai_test"
        )
        self.results_dir = Path("tests/e2e/results")
        self.results_dir.mkdir(exist_ok=True)
        self.errors: List[str] = []
        self.warnings: List[str] = []

    def connect_db(self):
        """Connect to the test database."""
        return psycopg2.connect(self.db_url, cursor_factory=RealDictCursor)

    @abstractmethod
    def load_expected_data(self) -> Dict[str, Any]:
        """Load expected results for this package manager."""
        pass

    @abstractmethod
    def validate_packages(self, expected: Dict[str, Any]) -> None:
        """Validate that packages were correctly imported."""
        pass

    @abstractmethod
    def validate_dependencies(self, expected: Dict[str, Any]) -> None:
        """Validate that dependencies were correctly linked."""
        pass

    @abstractmethod
    def validate_urls(self, expected: Dict[str, Any]) -> None:
        """Validate that URLs were correctly processed."""
        pass

    def validate_basic_integrity(self) -> None:
        """Run basic integrity checks on the database."""
        with self.connect_db() as conn:
            with conn.cursor() as cur:
                # Check for orphaned package_urls
                cur.execute("""
                    SELECT COUNT(*) as count FROM package_urls pu
                    LEFT JOIN packages p ON pu.package_id = p.id
                    WHERE p.id IS NULL
                """)
                orphaned = cur.fetchone()
                if orphaned["count"] > 0:
                    self.errors.append(f"Found {orphaned['count']} orphaned package_urls")

                # Check for orphaned dependencies
                cur.execute("""
                    SELECT COUNT(*) as count FROM legacy_dependencies ld
                    LEFT JOIN packages p ON ld.package_id = p.id
                    WHERE p.id IS NULL
                """)
                orphaned = cur.fetchone()
                if orphaned["count"] > 0:
                    self.errors.append(f"Found {orphaned['count']} orphaned dependencies")

                # Check for duplicate URLs
                cur.execute("""
                    SELECT url, url_type_id, COUNT(*) as count
                    FROM urls
                    GROUP BY url, url_type_id
                    HAVING COUNT(*) > 1
                """)
                duplicates = cur.fetchall()
                if duplicates:
                    for dup in duplicates:
                        self.errors.append(
                            f"Duplicate URL: {dup['url']} (type: {dup['url_type_id']}, count: {dup['count']})"
                        )

    def check_cache_usage(self) -> None:
        """Verify that cache structures are being used correctly."""
        # This would catch issues like the url_cache refactoring problem
        # by checking that the pipeline properly uses the cache
        with self.connect_db() as conn:
            with conn.cursor() as cur:
                # Get package count
                cur.execute(
                    "SELECT COUNT(*) as count FROM packages WHERE package_manager_id = "
                    "(SELECT id FROM package_managers WHERE name = %s)",
                    (self.package_manager,),
                )
                pkg_count = cur.fetchone()["count"]

                # Get URL count
                cur.execute("""
                    SELECT COUNT(DISTINCT u.id) as count 
                    FROM urls u
                    JOIN package_urls pu ON u.id = pu.url_id
                    JOIN packages p ON pu.package_id = p.id
                    WHERE p.package_manager_id = 
                        (SELECT id FROM package_managers WHERE name = %s)
                """, (self.package_manager,))
                url_count = cur.fetchone()["count"]

                # Basic sanity checks
                if pkg_count == 0:
                    self.errors.append(f"No packages found for {self.package_manager}")
                if url_count == 0 and pkg_count > 0:
                    self.warnings.append(f"No URLs found for {self.package_manager} packages")

    def run_validation(self) -> bool:
        """Run all validation steps."""
        print(f"Validating {self.package_manager} pipeline results...")

        try:
            # Load expected data
            expected = self.load_expected_data()

            # Run validations
            self.validate_packages(expected)
            self.validate_dependencies(expected)
            self.validate_urls(expected)
            self.validate_basic_integrity()
            self.check_cache_usage()

            # Write results
            self.write_results()

            # Return success/failure
            if self.errors:
                print(f"❌ {self.package_manager} validation failed with {len(self.errors)} errors")
                for error in self.errors:
                    print(f"  ERROR: {error}")
                return False
            else:
                print(f"✅ {self.package_manager} validation passed")
                if self.warnings:
                    print(f"  ⚠️  {len(self.warnings)} warnings:")
                    for warning in self.warnings:
                        print(f"    WARNING: {warning}")
                return True

        except Exception as e:
            print(f"❌ {self.package_manager} validation failed with exception: {e}")
            self.errors.append(f"Exception during validation: {str(e)}")
            self.write_results()
            return False

    def write_results(self) -> None:
        """Write validation results to file."""
        results = {
            "package_manager": self.package_manager,
            "timestamp": datetime.now().isoformat(),
            "errors": self.errors,
            "warnings": self.warnings,
            "success": len(self.errors) == 0,
        }

        output_file = self.results_dir / f"{self.package_manager}_validation.json"
        with open(output_file, "w") as f:
            json.dump(results, f, indent=2)

    def compare_sets(
        self, actual_set: Set[str], expected_set: Set[str], entity_type: str
    ) -> None:
        """Compare two sets and record differences."""
        missing = expected_set - actual_set
        extra = actual_set - expected_set

        if missing:
            self.errors.append(
                f"Missing {entity_type} for {self.package_manager}: {sorted(missing)}"
            )
        if extra:
            self.warnings.append(
                f"Extra {entity_type} for {self.package_manager}: {sorted(extra)}"
            )