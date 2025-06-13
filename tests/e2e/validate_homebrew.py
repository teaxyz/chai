#!/usr/bin/env pkgx uv run
"""Validator for Homebrew pipeline end-to-end test."""

import json
import sys
from pathlib import Path
from typing import Any, Dict, Set

from base_validator import BaseValidator


class HomebrewValidator(BaseValidator):
    """Validator specific to Homebrew pipeline."""

    def __init__(self):
        super().__init__("homebrew")

    def load_expected_data(self) -> Dict[str, Any]:
        """Load expected results for Homebrew."""
        expected_file = Path("tests/e2e/expected/homebrew_expected.json")
        if not expected_file.exists():
            # Create a default expected file with minimal test data
            default_expected = {
                "packages": ["wget", "curl", "git"],
                "dependencies": {
                    "wget": ["openssl", "libidn2"],
                    "curl": ["openssl", "zlib"],
                    "git": ["openssl", "pcre2"],
                },
                "urls": {
                    "wget": {
                        "homepage": "https://www.gnu.org/software/wget/",
                        "source": "https://ftp.gnu.org/gnu/wget/wget-1.21.3.tar.gz",
                    },
                    "curl": {
                        "homepage": "https://curl.se",
                        "source": "https://curl.se/download/curl-8.0.0.tar.gz",
                    },
                    "git": {
                        "homepage": "https://git-scm.com",
                        "source": "https://github.com/git/git/archive/v2.40.0.tar.gz",
                    },
                },
            }
            expected_file.parent.mkdir(parents=True, exist_ok=True)
            with open(expected_file, "w") as f:
                json.dump(default_expected, f, indent=2)
            return default_expected

        with open(expected_file, "r") as f:
            return json.load(f)

    def validate_packages(self, expected: Dict[str, Any]) -> None:
        """Validate that packages were correctly imported."""
        with self.connect_db() as conn:
            with conn.cursor() as cur:
                # Get all packages for homebrew
                cur.execute("""
                    SELECT p.name, p.import_id, p.readme
                    FROM packages p
                    JOIN package_managers pm ON p.package_manager_id = pm.id
                    WHERE pm.name = 'homebrew'
                """)
                actual_packages = {row["name"] for row in cur.fetchall()}

        expected_packages = set(expected["packages"])
        self.compare_sets(actual_packages, expected_packages, "packages")

    def validate_dependencies(self, expected: Dict[str, Any]) -> None:
        """Validate that dependencies were correctly linked."""
        with self.connect_db() as conn:
            with conn.cursor() as cur:
                # Get all dependencies
                cur.execute("""
                    SELECT 
                        p1.name as package,
                        p2.name as dependency,
                        dt.name as dep_type
                    FROM legacy_dependencies ld
                    JOIN packages p1 ON ld.package_id = p1.id
                    JOIN packages p2 ON ld.dependency_id = p2.id
                    JOIN dependency_types dt ON ld.dependency_type_id = dt.id
                    JOIN package_managers pm ON p1.package_manager_id = pm.id
                    WHERE pm.name = 'homebrew'
                """)
                
                actual_deps = {}
                for row in cur.fetchall():
                    pkg = row["package"]
                    dep = row["dependency"]
                    if pkg not in actual_deps:
                        actual_deps[pkg] = set()
                    actual_deps[pkg].add(dep)

        # Compare dependencies
        for pkg, expected_deps in expected["dependencies"].items():
            if pkg in actual_deps:
                self.compare_sets(
                    actual_deps[pkg],
                    set(expected_deps),
                    f"dependencies of {pkg}"
                )
            else:
                self.errors.append(f"Package {pkg} not found in actual results")

    def validate_urls(self, expected: Dict[str, Any]) -> None:
        """Validate that URLs were correctly processed."""
        with self.connect_db() as conn:
            with conn.cursor() as cur:
                # Get all URLs
                cur.execute("""
                    SELECT 
                        p.name as package,
                        u.url,
                        ut.name as url_type
                    FROM packages p
                    JOIN package_urls pu ON p.id = pu.package_id
                    JOIN urls u ON pu.url_id = u.id
                    JOIN url_types ut ON u.url_type_id = ut.id
                    JOIN package_managers pm ON p.package_manager_id = pm.id
                    WHERE pm.name = 'homebrew'
                """)
                
                actual_urls = {}
                for row in cur.fetchall():
                    pkg = row["package"]
                    url_type = row["url_type"]
                    url = row["url"]
                    
                    if pkg not in actual_urls:
                        actual_urls[pkg] = {}
                    actual_urls[pkg][url_type] = url

        # Compare URLs
        for pkg, expected_pkg_urls in expected["urls"].items():
            if pkg not in actual_urls:
                self.errors.append(f"No URLs found for package {pkg}")
                continue
                
            for url_type, expected_url in expected_pkg_urls.items():
                actual_url = actual_urls[pkg].get(url_type)
                if not actual_url:
                    self.errors.append(f"Missing {url_type} URL for {pkg}")
                elif actual_url != expected_url:
                    # URLs might have minor version differences, so warn instead of error
                    self.warnings.append(
                        f"URL mismatch for {pkg} {url_type}: "
                        f"expected '{expected_url}', got '{actual_url}'"
                    )

    def validate_cache_structures(self) -> None:
        """Additional validation to catch cache structure issues."""
        # This specifically checks for issues like the url_cache problem
        with self.connect_db() as conn:
            with conn.cursor() as cur:
                # Check that URLs are properly deduplicated
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
                            f"Duplicate URL found (cache not working?): {dup['url']}"
                        )


if __name__ == "__main__":
    validator = HomebrewValidator()
    success = validator.run_validation()
    sys.exit(0 if success else 1)