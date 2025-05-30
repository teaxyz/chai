import unittest
from datetime import datetime
from unittest.mock import MagicMock, patch
from uuid import uuid4

from core.config import URLTypes
from core.models import URL, Canon, Package
from ranker.config import Config
from ranker.dedupe_v2 import DedupeDB, main


class TestDedupe(unittest.TestCase):
    """
    Test the deduplication of packages - focused on case 2d (URL has no canon)
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

    @patch("ranker.dedupe_v2.is_canonical_url")
    def test_case_2d_new_canon_new_mapping(self, mock_is_canonical):
        """
        Test Case 2d: URL has no canon AND package has no existing mapping

        Expected: Create new canon + create new mapping
        """
        # Arrange
        mock_is_canonical.return_value = True

        # Create URL object that has no canon
        homepage_url = URL(
            id=self.url1_id,
            url=self.canonical_url,
            url_type_id=self.homepage_url_type_id,
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )

        # Current state: no canons exist for this URL, no package mapping exists
        current_canons = {}  # URL has no canon
        current_canon_packages = {}  # Package has no mapping
        packages_with_homepages = [(self.package1, homepage_url)]

        # Mock database
        mock_db = MagicMock(spec=DedupeDB)
        mock_db.get_current_canons.return_value = current_canons
        mock_db.get_current_canon_packages.return_value = current_canon_packages
        mock_db.get_packages_with_homepages.return_value = packages_with_homepages

        created_canons = []
        created_mappings = []
        updated_mappings = []

        def capture_canon(canon):
            created_canons.append(canon)

        def capture_mapping(mapping):
            created_mappings.append(mapping)

        def capture_mapping_update(mapping):
            updated_mappings.append(mapping)

        mock_db.create_canon.side_effect = capture_canon
        mock_db.create_canon_package.side_effect = capture_mapping
        mock_db.update_canon_package.side_effect = capture_mapping_update

        # Act
        with patch.dict("os.environ", {"LOAD": "true", "TEST": "false"}):
            main(self.mock_config, mock_db)

        # Assert
        self.assertEqual(len(created_canons), 1, "Should create exactly one new canon")
        self.assertEqual(
            len(created_mappings), 1, "Should create exactly one new mapping"
        )

        # Verify canon creation
        created_canon = created_canons[0]
        self.assertEqual(
            created_canon.url_id, self.url1_id, "Canon should reference correct URL ID"
        )
        self.assertEqual(
            created_canon.name, self.canonical_url, "Canon name should be URL"
        )

        # Verify mapping creation
        created_mapping = created_mappings[0]
        self.assertEqual(
            created_mapping.package_id, self.pkg1_id, "Should map correct package"
        )
        self.assertEqual(
            created_mapping.canon_id, created_canon.id, "Should map to new canon"
        )

        # Verify no updates to existing data
        mock_db.update_canon_package.assert_not_called()

    @patch("ranker.dedupe_v2.is_canonical_url")
    def test_case_2d_new_canon_update_mapping(self, mock_is_canonical):
        """
        Test Case 2d: URL has no canon AND package has existing mapping to different canon

        Expected: Create new canon + update existing mapping
        """
        # Arrange
        mock_is_canonical.return_value = True

        # Create URL object that has no canon
        homepage_url = URL(
            id=self.url1_id,
            url=self.canonical_url,
            url_type_id=self.homepage_url_type_id,
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )

        # Create existing canon for different URL
        existing_canon = Canon(
            id=self.canon2_id,
            url_id=self.url2_id,  # Different URL
            name="old-canon",
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )

        # Current state: no canon for this URL, but package is mapped to different canon
        current_canons = {self.url2_id: existing_canon}  # old URL exists
        current_canon_packages = {self.pkg1_id: existing_canon.id}  # pkg mapped to old
        packages_with_homepages = [(self.package1, homepage_url)]  # homepage is new

        # Mock database
        mock_db = MagicMock(spec=DedupeDB)
        mock_db.get_current_canons.return_value = current_canons
        mock_db.get_current_canon_packages.return_value = current_canon_packages
        mock_db.get_packages_with_homepages.return_value = packages_with_homepages

        created_canons = []
        created_mappings = []
        updated_mappings = []

        def capture_canon(canon):
            created_canons.append(canon)

        def capture_mapping(mapping):
            created_mappings.append(mapping)

        def capture_mapping_update(mapping):
            updated_mappings.append(mapping)

        mock_db.create_canon.side_effect = capture_canon
        mock_db.create_canon_package.side_effect = capture_mapping
        mock_db.update_canon_package.side_effect = capture_mapping_update

        # Act
        with patch.dict("os.environ", {"LOAD": "true", "TEST": "false"}):
            main(self.mock_config, mock_db)

        # Assert
        self.assertEqual(len(created_canons), 1, "Should create exactly one new canon")
        self.assertEqual(len(updated_mappings), 1, "Should update exactly one mapping")

        # Verify canon creation
        created_canon = created_canons[0]
        self.assertEqual(
            created_canon.url_id, self.url1_id, "Canon should reference correct URL ID"
        )
        self.assertEqual(
            created_canon.name, self.canonical_url, "Canon name should be URL"
        )

        # Verify mapping update (should point to NEW canon, not old one)
        updated_mapping = updated_mappings[0]
        self.assertEqual(
            updated_mapping[0], self.pkg1_id, "Should update correct package"
        )
        self.assertEqual(
            updated_mapping[1],
            created_canon.id,
            "Should update to NEW canon, not old one",
        )
        self.assertNotEqual(
            updated_mapping[1], self.canon2_id, "Should NOT point to old canon"
        )

        # Verify no updates to existing canons
        mock_db.create_canon_package.assert_not_called()

    @patch("ranker.dedupe_v2.is_canonical_url")
    def test_case_2a_no_changes_needed(self, mock_is_canonical):
        """
        Test Case 2a: URL has canon AND package already linked to that canon

        Expected: Do nothing (no changes)
        """
        # Arrange
        mock_is_canonical.return_value = True

        # Create URL object and existing canon
        homepage_url = URL(
            id=self.url1_id,
            url=self.canonical_url,
            url_type_id=self.homepage_url_type_id,
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )

        existing_canon = Canon(
            id=self.canon1_id,
            url_id=self.url1_id,
            name="existing-canon",
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )

        # Current state: URL has canon, package linked to that same canon
        current_canons = {self.url1_id: existing_canon}
        current_canon_packages = {self.pkg1_id: self.canon1_id}  # Already correct
        packages_with_homepages = [(self.package1, homepage_url)]

        # Mock database
        mock_db = MagicMock(spec=DedupeDB)
        mock_db.get_current_canons.return_value = current_canons
        mock_db.get_current_canon_packages.return_value = current_canon_packages
        mock_db.get_packages_with_homepages.return_value = packages_with_homepages

        # Act
        with patch.dict("os.environ", {"LOAD": "true", "TEST": "false"}):
            main(self.mock_config, mock_db)

        # Assert - no changes should be made
        mock_db.create_canon.assert_not_called()
        mock_db.create_canon_package.assert_not_called()
        mock_db.update_canon_package.assert_not_called()

    @patch("ranker.dedupe_v2.is_canonical_url")
    def test_case_2b_update_existing_mapping(self, mock_is_canonical):
        """
        Test Case 2b: URL has canon AND package linked to different canon

        Expected: Update mapping to correct canon
        """
        # Arrange
        mock_is_canonical.return_value = True

        # Create URL object and canons
        homepage_url = URL(
            id=self.url1_id,
            url=self.canonical_url,
            url_type_id=self.homepage_url_type_id,
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )

        correct_canon = Canon(
            id=self.canon1_id,
            url_id=self.url1_id,  # This URL's canon
            name="correct-canon",
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )

        wrong_canon = Canon(
            id=self.canon2_id,
            url_id=self.url2_id,  # Different URL's canon
            name="wrong-canon",
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )

        # Current state: URL has canon, but package linked to wrong canon
        current_canons = {
            self.url1_id: correct_canon,
            self.url2_id: wrong_canon,
        }
        current_canon_packages = {self.pkg1_id: self.canon2_id}  # Linked to wrong canon
        packages_with_homepages = [(self.package1, homepage_url)]

        # Mock database
        mock_db = MagicMock(spec=DedupeDB)
        mock_db.get_current_canons.return_value = current_canons
        mock_db.get_current_canon_packages.return_value = current_canon_packages
        mock_db.get_packages_with_homepages.return_value = packages_with_homepages

        updated_mappings = []

        def capture_mapping_update(mapping):
            updated_mappings.append(mapping)

        mock_db.update_canon_package.side_effect = capture_mapping_update

        # Act
        with patch.dict("os.environ", {"LOAD": "true", "TEST": "false"}):
            main(self.mock_config, mock_db)

        # Assert
        self.assertEqual(len(updated_mappings), 1, "Should update exactly one mapping")

        # Verify mapping update points to correct canon
        updated_mapping = updated_mappings[0]
        self.assertEqual(
            updated_mapping[0], self.pkg1_id, "Should update correct package"
        )
        self.assertEqual(
            updated_mapping[1], self.canon1_id, "Should update to correct canon"
        )
        self.assertNotEqual(
            updated_mapping[1], self.canon2_id, "Should NOT point to wrong canon"
        )

        # Verify no new creations
        mock_db.create_canon.assert_not_called()
        mock_db.create_canon_package.assert_not_called()

    @patch("ranker.dedupe_v2.is_canonical_url")
    def test_case_2c_create_new_mapping(self, mock_is_canonical):
        """
        Test Case 2c: URL has canon AND package has no mapping

        Expected: Create new mapping to existing canon
        """
        # Arrange
        mock_is_canonical.return_value = True

        # Create URL object and existing canon
        homepage_url = URL(
            id=self.url1_id,
            url=self.canonical_url,
            url_type_id=self.homepage_url_type_id,
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )

        existing_canon = Canon(
            id=self.canon1_id,
            url_id=self.url1_id,
            name="existing-canon",
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )

        # Current state: URL has canon, but package has no mapping
        current_canons = {self.url1_id: existing_canon}
        current_canon_packages = {}  # Package not linked to anything
        packages_with_homepages = [(self.package1, homepage_url)]

        # Mock database
        mock_db = MagicMock(spec=DedupeDB)
        mock_db.get_current_canons.return_value = current_canons
        mock_db.get_current_canon_packages.return_value = current_canon_packages
        mock_db.get_packages_with_homepages.return_value = packages_with_homepages

        created_mappings = []

        def capture_mapping(mapping):
            created_mappings.append(mapping)

        mock_db.create_canon_package.side_effect = capture_mapping

        # Act
        with patch.dict("os.environ", {"LOAD": "true", "TEST": "false"}):
            main(self.mock_config, mock_db)

        # Assert
        self.assertEqual(
            len(created_mappings), 1, "Should create exactly one new mapping"
        )

        # Verify mapping creation points to existing canon
        created_mapping = created_mappings[0]
        self.assertEqual(
            created_mapping.package_id, self.pkg1_id, "Should map correct package"
        )
        self.assertEqual(
            created_mapping.canon_id, self.canon1_id, "Should map to existing canon"
        )

        # Verify no other changes
        mock_db.create_canon.assert_not_called()
        mock_db.update_canon_package.assert_not_called()


if __name__ == "__main__":
    unittest.main()
