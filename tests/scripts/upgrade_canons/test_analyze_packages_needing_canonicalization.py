#!/usr/bin/env pkgx uv run

from unittest.mock import call, patch
from uuid import UUID

import pytest

from scripts.upgrade_canons.main import analyze_packages_needing_canonicalization


class TestAnalyzePackagesNeedingCanonicalization:
    """Test the analyze_packages_needing_canonicalization function"""

    def setup_method(self):
        """Setup test fixtures"""
        self.package_id_1 = UUID("11111111-1111-1111-1111-111111111111")
        self.package_id_2 = UUID("22222222-2222-2222-2222-222222222222")
        self.package_id_3 = UUID("33333333-3333-3333-3333-333333333333")
        self.package_id_4 = UUID("44444444-4444-4444-4444-444444444444")

    @patch("scripts.upgrade_canons.main.is_canonical_url")
    @patch("scripts.upgrade_canons.main.normalize_url")
    def test_case_1_should_create_canonical_url(
        self, mock_normalize, mock_is_canonical
    ):
        """
        Test Case 1: Package has non-canonical URLs, canonical doesn't exist
        Expected: Should return this package in the result
        """
        # Setup mocks
        mock_is_canonical.return_value = False
        mock_normalize.return_value = "github.com/org/repo"

        # Test data
        package_url_map = {
            self.package_id_1: [
                "https://github.com/org/repo",
                "https://github.com/org/repo/tree/main",
                "https://github.com/org/repo/blob/main/README.md",
            ]
        }
        existing_homepages = {
            "https://github.com/org/repo",
            "https://github.com/org/repo/tree/main",
            "https://github.com/org/repo/blob/main/README.md",
        }  # no canon

        # Execute
        result = analyze_packages_needing_canonicalization(
            package_url_map, existing_homepages
        )

        # Verify
        assert len(result) == 1
        assert self.package_id_1 in result
        assert result[self.package_id_1] == "github.com/org/repo"

        # Verify mocks were called correctly
        # is_canonical should be called once for each URL until it finds a canonical one (or all if none are canonical)
        expected_calls = [
            call("https://github.com/org/repo"),
            call("https://github.com/org/repo/tree/main"),
            call("https://github.com/org/repo/blob/main/README.md"),
        ]
        mock_is_canonical.assert_has_calls(expected_calls)
        assert mock_is_canonical.call_count == 3

        # normalize should only be called once with the first URL
        mock_normalize.assert_called_once_with("https://github.com/org/repo")

    @patch("scripts.upgrade_canons.main.is_canonical_url")
    @patch("scripts.upgrade_canons.main.normalize_url")
    def test_case_2_canonical_exists_in_database(
        self, mock_normalize, mock_is_canonical
    ):
        """
        Test Case 2: Package has non-canonical URLs, but canonical already exists in DB
        Expected: Should not return this package (skip it)
        """
        # Setup mocks
        mock_is_canonical.return_value = False
        mock_normalize.return_value = "https://example.com"

        # Test data
        package_url_map = {
            self.package_id_1: ["http://example.com", "https://www.example.com"]
        }
        existing_homepages = {"https://example.com"}  # Canonical already exists

        # Execute
        result = analyze_packages_needing_canonicalization(
            package_url_map, existing_homepages
        )

        # Verify
        assert len(result) == 0
        assert self.package_id_1 not in result

    @patch("scripts.upgrade_canons.main.is_canonical_url")
    @patch("scripts.upgrade_canons.main.normalize_url")
    def test_case_3_canonical_already_planned(self, mock_normalize, mock_is_canonical):
        """
        Test Case 3: Two packages would create the same canonical URL
        Expected: Only the first package should be included, second should be skipped
        """
        # Setup mocks
        mock_is_canonical.return_value = False
        mock_normalize.return_value = (
            "https://example.com"  # Both packages normalize to same URL
        )

        # Test data - both packages would create the same canonical URL
        package_url_map = {
            self.package_id_1: ["http://example.com"],
            self.package_id_2: [
                "https://www.example.com"
            ],  # Different input, same canonical
        }
        existing_homepages = set()  # Empty - canonical doesn't exist

        # Execute
        result = analyze_packages_needing_canonicalization(
            package_url_map, existing_homepages
        )

        # Verify - only one package should be included (whichever was processed first)
        assert len(result) == 1
        assert "https://example.com" in result.values()

        # Verify that exactly one of the packages was included
        included_packages = list(result.keys())
        assert len(included_packages) == 1
        assert included_packages[0] in [self.package_id_1, self.package_id_2]

    @patch("scripts.upgrade_canons.main.is_canonical_url")
    def test_case_4_package_already_has_canonical(self, mock_is_canonical):
        """
        Test Case 4: Package already has at least one canonical URL
        Expected: Should not return this package (skip it)
        """
        # Setup mocks - return True for canonical check
        mock_is_canonical.return_value = True

        # Test data
        package_url_map = {
            self.package_id_1: [
                "https://example.com",
                "http://example.com",
            ]  # First URL is canonical
        }
        existing_homepages = set()

        # Execute
        result = analyze_packages_needing_canonicalization(
            package_url_map, existing_homepages
        )

        # Verify
        assert len(result) == 0
        assert self.package_id_1 not in result

        # Verify that we never tried to normalize (because we skipped early)
        mock_is_canonical.assert_called_once_with("https://example.com")

    @patch("scripts.upgrade_canons.main.is_canonical_url")
    @patch("scripts.upgrade_canons.main.normalize_url")
    def test_mixed_scenarios(self, mock_normalize, mock_is_canonical):
        """
        Test with multiple packages covering different scenarios
        """

        # Setup mocks with side effects for different URLs
        def mock_is_canonical_side_effect(url):
            return url == "https://canonical.com"  # Only this URL is canonical

        def mock_normalize_side_effect(url):
            if "example" in url:
                return "https://example.com"
            elif "test" in url:
                return "https://test.com"
            else:
                return f"https://{url.split('://')[1]}"

        mock_is_canonical.side_effect = mock_is_canonical_side_effect
        mock_normalize.side_effect = mock_normalize_side_effect

        # Test data
        package_url_map = {
            self.package_id_1: ["http://example.com"],  # Should create canonical
            self.package_id_2: ["https://canonical.com"],  # Already canonical - skip
            self.package_id_3: ["http://test.com"],  # Should create canonical
            self.package_id_4: [
                "https://www.example.com"
            ],  # Same canonical as package_id_1 - skip
        }
        existing_homepages = set()

        # Execute
        result = analyze_packages_needing_canonicalization(
            package_url_map, existing_homepages
        )

        # Verify
        assert len(result) == 2

        # Package 1 should be included (creates https://example.com)
        assert self.package_id_1 in result
        assert result[self.package_id_1] == "https://example.com"

        # Package 2 should be skipped (already canonical)
        assert self.package_id_2 not in result

        # Package 3 should be included (creates https://test.com)
        assert self.package_id_3 in result
        assert result[self.package_id_3] == "https://test.com"

        # Package 4 should be skipped (duplicate canonical URL)
        assert self.package_id_4 not in result

    def test_empty_inputs(self):
        """Test with empty inputs"""
        result = analyze_packages_needing_canonicalization({}, set())
        assert result == {}

    @patch("scripts.upgrade_canons.main.is_canonical_url")
    @patch("scripts.upgrade_canons.main.normalize_url")
    def test_edge_case_empty_url_list(self, mock_normalize, mock_is_canonical):
        """Test with package that has empty URL list"""
        # This shouldn't happen in practice, but let's handle it gracefully
        package_url_map = {
            self.package_id_1: []  # Empty URL list
        }
        existing_homepages = set()

        # This will raise an IndexError when trying to access urls[0] in generate_canonical_url
        # Let's verify this behavior is expected
        with pytest.raises(IndexError):
            analyze_packages_needing_canonicalization(
                package_url_map, existing_homepages
            )


if __name__ == "__main__":
    pytest.main([__file__])
