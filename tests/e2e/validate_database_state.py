#!/usr/bin/env pkgx uv run
"""Validate overall database state after all pipeline runs."""

import json
import sys
from datetime import datetime
from pathlib import Path

import psycopg2
from psycopg2.extras import RealDictCursor


class DatabaseStateValidator:
    """Validates the overall database state after all pipelines."""

    def __init__(self):
        self.db_url = "postgresql://postgres:test_password@localhost:5432/chai_test"
        self.errors = []
        self.warnings = []
        self.results_dir = Path("tests/e2e/results")
        self.results_dir.mkdir(exist_ok=True)

    def connect_db(self):
        """Connect to the test database."""
        return psycopg2.connect(self.db_url, cursor_factory=RealDictCursor)

    def validate_referential_integrity(self):
        """Check that all foreign key relationships are valid."""
        print("Checking referential integrity...")
        
        with self.connect_db() as conn:
            with conn.cursor() as cur:
                # Check package_urls -> packages
                cur.execute("""
                    SELECT COUNT(*) as count FROM package_urls pu
                    LEFT JOIN packages p ON pu.package_id = p.id
                    WHERE p.id IS NULL
                """)
                orphaned = cur.fetchone()
                if orphaned["count"] > 0:
                    self.errors.append(f"Found {orphaned['count']} orphaned package_urls")

                # Check package_urls -> urls
                cur.execute("""
                    SELECT COUNT(*) as count FROM package_urls pu
                    LEFT JOIN urls u ON pu.url_id = u.id
                    WHERE u.id IS NULL
                """)
                orphaned = cur.fetchone()
                if orphaned["count"] > 0:
                    self.errors.append(f"Found {orphaned['count']} package_urls with missing URLs")

                # Check legacy_dependencies -> packages (package_id)
                cur.execute("""
                    SELECT COUNT(*) as count FROM legacy_dependencies ld
                    LEFT JOIN packages p ON ld.package_id = p.id
                    WHERE p.id IS NULL
                """)
                orphaned = cur.fetchone()
                if orphaned["count"] > 0:
                    self.errors.append(f"Found {orphaned['count']} dependencies with missing package_id")

                # Check legacy_dependencies -> packages (dependency_id)
                cur.execute("""
                    SELECT COUNT(*) as count FROM legacy_dependencies ld
                    LEFT JOIN packages p ON ld.dependency_id = p.id
                    WHERE p.id IS NULL
                """)
                orphaned = cur.fetchone()
                if orphaned["count"] > 0:
                    self.errors.append(f"Found {orphaned['count']} dependencies with missing dependency_id")

    def validate_no_duplicates(self):
        """Check for duplicate entries that shouldn't exist."""
        print("Checking for duplicates...")
        
        with self.connect_db() as conn:
            with conn.cursor() as cur:
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

                # Check for duplicate packages within a package manager
                cur.execute("""
                    SELECT pm.name as pm_name, p.import_id, COUNT(*) as count
                    FROM packages p
                    JOIN package_managers pm ON p.package_manager_id = pm.id
                    GROUP BY pm.name, p.import_id
                    HAVING COUNT(*) > 1
                """)
                duplicates = cur.fetchall()
                if duplicates:
                    for dup in duplicates:
                        self.errors.append(
                            f"Duplicate package: {dup['import_id']} in {dup['pm_name']} (count: {dup['count']})"
                        )

    def validate_cache_effectiveness(self):
        """Check that caching is working properly."""
        print("Checking cache effectiveness...")
        
        with self.connect_db() as conn:
            with conn.cursor() as cur:
                # Count unique URLs vs total package_urls
                cur.execute("SELECT COUNT(*) as count FROM urls")
                unique_urls = cur.fetchone()["count"]
                
                cur.execute("SELECT COUNT(*) as count FROM package_urls")
                total_package_urls = cur.fetchone()["count"]
                
                if total_package_urls > 0:
                    reuse_ratio = (total_package_urls - unique_urls) / total_package_urls
                    print(f"  URL reuse ratio: {reuse_ratio:.2%}")
                    
                    if reuse_ratio < 0.1:
                        self.warnings.append(
                            f"Low URL reuse ratio ({reuse_ratio:.2%}), cache might not be working properly"
                        )

    def validate_data_completeness(self):
        """Check that all package managers have loaded data."""
        print("Checking data completeness...")
        
        with self.connect_db() as conn:
            with conn.cursor() as cur:
                # Check each package manager
                cur.execute("SELECT name FROM package_managers")
                pms = cur.fetchall()
                
                for pm in pms:
                    pm_name = pm["name"]
                    
                    # Count packages
                    cur.execute("""
                        SELECT COUNT(*) as count FROM packages
                        WHERE package_manager_id = (SELECT id FROM package_managers WHERE name = %s)
                    """, (pm_name,))
                    pkg_count = cur.fetchone()["count"]
                    
                    if pkg_count == 0:
                        self.errors.append(f"No packages found for {pm_name}")
                    else:
                        print(f"  {pm_name}: {pkg_count} packages")

    def generate_summary_report(self):
        """Generate a summary of all validations."""
        print("\nGenerating summary report...")
        
        # Load individual validation results
        validation_results = {}
        for result_file in self.results_dir.glob("*_validation.json"):
            with open(result_file, "r") as f:
                data = json.load(f)
                validation_results[data["package_manager"]] = data

        # Create summary
        summary = {
            "timestamp": datetime.now().isoformat(),
            "database_state": {
                "errors": self.errors,
                "warnings": self.warnings,
                "success": len(self.errors) == 0,
            },
            "pipeline_validations": validation_results,
            "overall_success": len(self.errors) == 0 and all(
                v["success"] for v in validation_results.values()
            ),
        }

        # Write summary
        summary_file = self.results_dir / "validation_summary.json"
        with open(summary_file, "w") as f:
            json.dump(summary, f, indent=2)

        return summary["overall_success"]

    def run(self):
        """Run all database state validations."""
        print("Running database state validation...")
        
        self.validate_referential_integrity()
        self.validate_no_duplicates()
        self.validate_cache_effectiveness()
        self.validate_data_completeness()
        
        success = self.generate_summary_report()
        
        if self.errors:
            print(f"\n❌ Database validation failed with {len(self.errors)} errors:")
            for error in self.errors:
                print(f"  ERROR: {error}")
        else:
            print("\n✅ Database validation passed")
            
        if self.warnings:
            print(f"\n⚠️  {len(self.warnings)} warnings:")
            for warning in self.warnings:
                print(f"  WARNING: {warning}")

        return success


if __name__ == "__main__":
    validator = DatabaseStateValidator()
    success = validator.run()
    sys.exit(0 if success else 1)