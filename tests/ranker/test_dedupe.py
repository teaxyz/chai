import unittest
from datetime import datetime
from unittest.mock import MagicMock, patch
from uuid import uuid4

from core.config import URLTypes
from core.models import URL, Canon, Package
from ranker.config import Config
from ranker.dedupe_v2 import (
    DedupeDB,
    find_canon_for_url,
    get_latest_homepage_per_package,
    main,
)


class TestDedupe(unittest.TestCase):
    """
    Test the deduplication of packages
    """

    def setUp(self):
        """Set up common test data and mocks."""
        # Create fixed UUIDs for consistent testing
        self.homepage_url_type_id = uuid4()
        self.package_manager_id = uuid4()

        # Create fixed package/canon IDs
        self.pkg1_id = uuid4()
        self.pkg2_id = uuid4()
        self.pkg3_id = uuid4()

        self.canon1_id = uuid4()
        self.canon2_id = uuid4()
        self.canon3_id = uuid4()

        self.url1_id = uuid4()
        self.url2_id = uuid4()
        self.url3_id = uuid4()

        # Mock config
        self.url_types = MagicMock(spec=URLTypes)
        self.url_types.homepage_url_type_id = self.homepage_url_type_id
        self.mock_config = MagicMock(spec=Config)
        self.mock_config.url_types = self.url_types

        # Common test URLs
        self.canonical_url = "github.com/example/repo"
        self.non_canonical_url = "https://github.com/example/repo"
        self.different_url = "https://gitlab.com/example/repo"

        # Common test objects
        self.package1 = Package(
            id=self.pkg1_id,
            name="package1",
            package_manager_id=self.package_manager_id,
            import_id="pkg1",
            derived_id="npm/package1",
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )

        self.package2 = Package(
            id=self.pkg2_id,
            name="package2",
            package_manager_id=self.package_manager_id,
            import_id="pkg2",
            derived_id="npm/package2",
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )

        self.package3 = Package(
            id=self.pkg3_id,
            name="package3",
            package_manager_id=self.package_manager_id,
            import_id="pkg3",
            derived_id="npm/package3",
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )

    @patch("ranker.dedupe_v2.normalize_url")
    def test_case_1_new_package_new_canon(self, mock_normalize):
        """
        Test Case 1: Package has homepage URL that doesn't exist as canon,
        and package has no existing canon mapping.

        Expected: New canon + new canon package link
        """
        # Arrange
        mock_normalize.return_value = self.canonical_url  # URL is already canonical

        # Create URL object
        homepage_url = URL(
            id=self.url1_id,
            url=self.canonical_url,
            url_type_id=self.homepage_url_type_id,
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )

        # Empty current state - no existing canons or mappings
        current_canons = {}
        current_canon_packages = {}
        # But there is a package with a homepage URL
        packages_with_homepages = [(self.package1, homepage_url)]

        # Mock database
        mock_db = MagicMock(spec=DedupeDB)
        mock_db.get_current_canons.return_value = current_canons
        mock_db.get_current_canon_packages.return_value = current_canon_packages
        mock_db.get_packages_with_homepages.return_value = packages_with_homepages

        created_canons = []
        created_mappings = []

        def capture_canon(canon):
            created_canons.append(canon)

        def capture_mapping(package_id, canon_id):
            created_mappings.append((package_id, canon_id))

        mock_db.create_canon.side_effect = capture_canon
        mock_db.update_canon_package_mapping.side_effect = capture_mapping

        # Act
        # Use LOAD=true to make changes to the database, but mock_db.create_canon has a
        # side effect that captures the canon, so nothing gets created
        with patch.dict("os.environ", {"LOAD": "true", "TEST": "false"}):
            main(self.mock_config, mock_db)

        # Assert
        self.assertEqual(len(created_canons), 1, "Should create exactly one new canon")
        self.assertEqual(
            len(created_mappings), 1, "Should create exactly one canon-package mapping"
        )

        created_canon = created_canons[0]
        self.assertEqual(created_canon.url, self.canonical_url)
        self.assertEqual(created_canon.name, "package1")

        created_mapping = created_mappings[0]
        self.assertEqual(created_mapping[0], self.pkg1_id)
        self.assertEqual(created_mapping[1], created_canon.id)

        # Verify no updates to existing canons
        mock_db.update_canon_url.assert_not_called()

    @patch("ranker.dedupe_v2.normalize_url")
    def test_case_2_canonicalized_url_update_existing_canon(self, mock_normalize):
        """
        Test Case 2: Package has canonicalized homepage URL, but existing canon
        has non-canonicalized version. Package is linked to existing canon.

        Expected: Update existing canon's URL to canonicalized version
        """
        # Arrange
        mock_normalize.return_value = self.canonical_url

        # Create existing canon with non-canonical URL
        existing_canon = Canon(
            id=self.canon1_id,
            url=self.non_canonical_url,  # https://github.com/example/repo
            name="package1",
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )

        # Create URL object with canonical URL
        homepage_url = URL(
            id=self.url1_id,
            url=self.canonical_url,
            url_type_id=self.homepage_url_type_id,
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )

        # Current state: canon exists, package is mapped to it
        current_canons = {self.non_canonical_url: existing_canon}
        current_canon_packages = {self.pkg1_id: self.canon1_id}
        packages_with_homepages = [(self.package1, homepage_url)]

        # Mock database
        mock_db = MagicMock(spec=DedupeDB)
        mock_db.get_current_canons.return_value = current_canons
        mock_db.get_current_canon_packages.return_value = current_canon_packages
        mock_db.get_packages_with_homepages.return_value = packages_with_homepages

        updated_canons = []

        def capture_canon_update(canon_id, new_url):
            updated_canons.append((canon_id, new_url))

        mock_db.update_canon_url.side_effect = capture_canon_update

        # Act
        with patch.dict("os.environ", {"LOAD": "true", "TEST": "false"}):
            main(self.mock_config, mock_db)

        # Assert
        self.assertEqual(len(updated_canons), 1, "Should update exactly one canon URL")

        updated_canon = updated_canons[0]
        self.assertEqual(updated_canon[0], self.canon1_id)
        self.assertEqual(updated_canon[1], self.canonical_url)

        # Verify no new canons or mappings created
        mock_db.create_canon.assert_not_called()
        mock_db.update_canon_package_mapping.assert_not_called()

    @patch("ranker.dedupe_v2.normalize_url")
    @patch("ranker.dedupe_v2.Logger")
    def test_case_3_duplicate_canons_existing_mapping(
        self, mock_logger_class, mock_normalize
    ):
        """
        Test Case 3: Both canonical and non-canonical URLs exist as separate canons,
        package is linked to the non-canonical canon.

        Expected: Log warning about duplicate canons, no updates made
        """
        # Arrange
        mock_logger = MagicMock()
        mock_logger_class.return_value = mock_logger
        mock_normalize.return_value = self.canonical_url

        # Create two canons for effectively the same URL
        canon_canonical = Canon(
            id=self.canon1_id,
            url=self.canonical_url,
            name="package1",
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )

        canon_non_canonical = Canon(
            id=self.canon2_id,
            url=self.non_canonical_url,
            name="package1_alt",
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )

        # Create URL object
        homepage_url = URL(
            id=self.url1_id,
            url=self.canonical_url,
            url_type_id=self.homepage_url_type_id,
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )

        # Current state: both canons exist, package mapped to canonical one
        current_canons = {
            self.canonical_url: canon_canonical,
            self.non_canonical_url: canon_non_canonical,
        }
        current_canon_packages = {self.pkg1_id: self.canon2_id}
        packages_with_homepages = [(self.package1, homepage_url)]

        # Mock database
        mock_db = MagicMock(spec=DedupeDB)
        mock_db.get_current_canons.return_value = current_canons
        mock_db.get_current_canon_packages.return_value = current_canon_packages
        mock_db.get_packages_with_homepages.return_value = packages_with_homepages

        # Act
        with patch.dict("os.environ", {"LOAD": "true", "TEST": "false"}):
            main(self.mock_config, mock_db)

        # Assert
        # Should not make any database changes since this is a data problem
        mock_db.create_canon.assert_not_called()
        mock_db.update_canon_url.assert_not_called()
        mock_db.update_canon_package_mapping.assert_not_called()

    @patch("ranker.dedupe_v2.normalize_url")
    @patch("ranker.dedupe_v2.Logger")
    def test_case_4_duplicate_canons_no_mapping(
        self, mock_logger_class, mock_normalize
    ):
        """
        Test Case 4: Both canonical and non-canonical URLs exist as separate canons,
        but package is not linked to either.

        Expected: Log warning + create mapping to canonical canon
        """
        # Arrange
        mock_logger = MagicMock()
        mock_logger_class.return_value = mock_logger
        mock_normalize.return_value = self.canonical_url

        # Create two canons for effectively the same URL
        canon_canonical = Canon(
            id=self.canon1_id,
            url=self.canonical_url,
            name="package1",
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )

        canon_non_canonical = Canon(
            id=self.canon2_id,
            url=self.non_canonical_url,
            name="package1_alt",
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )

        # Create URL object
        homepage_url = URL(
            id=self.url1_id,
            url=self.canonical_url,
            url_type_id=self.homepage_url_type_id,
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )

        # Current state: both canons exist, but package has no mapping
        current_canons = {
            self.canonical_url: canon_canonical,
            self.non_canonical_url: canon_non_canonical,
        }
        current_canon_packages = {}  # No existing mappings
        packages_with_homepages = [(self.package1, homepage_url)]

        # Mock database
        mock_db = MagicMock(spec=DedupeDB)
        mock_db.get_current_canons.return_value = current_canons
        mock_db.get_current_canon_packages.return_value = current_canon_packages
        mock_db.get_packages_with_homepages.return_value = packages_with_homepages

        created_mappings = []

        def capture_mapping(package_id, canon_id):
            created_mappings.append((package_id, canon_id))

        mock_db.update_canon_package_mapping.side_effect = capture_mapping

        # Act
        with patch.dict("os.environ", {"LOAD": "true", "TEST": "false"}):
            main(self.mock_config, mock_db)

        # Assert
        self.assertEqual(
            len(created_mappings), 1, "Should create mapping to canonical canon"
        )

        created_mapping = created_mappings[0]
        self.assertEqual(created_mapping[0], self.pkg1_id)
        self.assertEqual(created_mapping[1], self.canon1_id)  # Maps to canonical canon

        # Should not create new canons or update existing ones
        mock_db.create_canon.assert_not_called()
        mock_db.update_canon_url.assert_not_called()

    def test_find_canon_for_url_exact_match(self):
        """Test find_canon_for_url with exact URL match."""
        canon = Canon(
            id=self.canon1_id,
            url=self.canonical_url,
            name="test",
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )

        current_canons = {self.canonical_url: canon}

        result = find_canon_for_url(self.canonical_url, current_canons)

        self.assertEqual(result, canon)

    @patch("ranker.dedupe_v2.normalize_url")
    def test_find_canon_for_url_canonicalized_match(self, mock_normalize):
        """Test find_canon_for_url with canonicalized URL match."""
        mock_normalize.return_value = self.canonical_url

        canon = Canon(
            id=self.canon1_id,
            url=self.non_canonical_url,
            name="test",
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )

        current_canons = {self.non_canonical_url: canon}

        result = find_canon_for_url(self.canonical_url, current_canons)

        self.assertEqual(result, canon)

    def test_find_canon_for_url_no_match(self):
        """Test find_canon_for_url with no matching canon."""
        canon = Canon(
            id=self.canon1_id,
            url=self.different_url,
            name="test",
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )

        current_canons = {self.different_url: canon}

        result = find_canon_for_url(self.canonical_url, current_canons)

        self.assertIsNone(result)

    def test_get_latest_homepage_per_package(self):
        """Test getting latest homepage per package from ordered list."""
        older_url = URL(
            id=self.url1_id,
            url="https://old.example.com",
            url_type_id=self.homepage_url_type_id,
            created_at=datetime(2023, 1, 1),
            updated_at=datetime(2023, 1, 1),
        )

        newer_url = URL(
            id=self.url2_id,
            url="https://new.example.com",
            url_type_id=self.homepage_url_type_id,
            created_at=datetime(2024, 1, 1),
            updated_at=datetime(2024, 1, 1),
        )

        # List should be ordered by Package.id, URL.created_at desc (newer first)
        packages_with_homepages = [
            (self.package1, newer_url),  # Newer URL first due to ordering
            (self.package1, older_url),  # Older URL second
            (self.package2, older_url),  # Different package
        ]

        result = get_latest_homepage_per_package(packages_with_homepages)

        # Should get the first (newest) URL for each package
        self.assertEqual(len(result), 2)
        self.assertEqual(result[self.pkg1_id], newer_url)
        self.assertEqual(result[self.pkg2_id], older_url)


if __name__ == "__main__":
    unittest.main()
