import unittest
from datetime import datetime
from unittest.mock import MagicMock, patch
from uuid import uuid4

# Mock the problematic modules before any imports that might trigger database calls
mock_config_db = MagicMock()
mock_config_db.get_npm_pm_id.return_value = uuid4()
mock_config_db.get_pm_id_by_name.return_value = [(uuid4(),)]

# Mock the config module
with patch.dict(
    "sys.modules",
    {
        "ranker.config": MagicMock(),
    },
):
    # Now import the modules we need
    from core.config import URLTypes
    from core.models import URL, Canon, Package
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
        self.mock_config = MagicMock()
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

    def test_case_2d_new_canon_new_mapping(self):
        """
        Test Case 2d: URL has no canon AND package has no existing mapping

        Expected: Create new canon + create new mapping
        """
        # Arrange
        # Create URL object that has no canon (using canonical URL to avoid validation issues)
        homepage_url = URL(
            id=self.url1_id,
            url=self.canonical_url,  # Already canonical
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

        # Capture ingest call arguments
        ingest_calls = []

        def capture_ingest(new_canons, new_canon_packages, updated_canon_packages):
            ingest_calls.append(
                (new_canons, new_canon_packages, updated_canon_packages)
            )

        mock_db.ingest.side_effect = capture_ingest

        # Act
        with patch.dict("os.environ", {"LOAD": "true", "TEST": "false"}):
            main(self.mock_config, mock_db)

        # Assert
        self.assertEqual(len(ingest_calls), 1, "Should call ingest exactly once")

        new_canons, new_canon_packages, updated_canon_packages = ingest_calls[0]

        # Verify canon creation
        self.assertEqual(len(new_canons), 1, "Should create exactly one new canon")
        self.assertEqual(
            len(new_canon_packages), 1, "Should create exactly one new mapping"
        )
        self.assertEqual(
            len(updated_canon_packages), 0, "Should not update any mappings"
        )

        created_canon = new_canons[0]
        self.assertEqual(
            created_canon.url_id, self.url1_id, "Canon should reference correct URL ID"
        )
        self.assertEqual(
            created_canon.name, self.canonical_url, "Canon name should be URL"
        )

        # Verify mapping creation
        created_mapping = new_canon_packages[0]
        self.assertEqual(
            created_mapping.package_id, self.pkg1_id, "Should map correct package"
        )
        self.assertEqual(
            created_mapping.canon_id, created_canon.id, "Should map to new canon"
        )

    def test_case_2d_new_canon_update_mapping(self):
        """
        Test Case 2d: URL has no canon AND package has existing mapping to different canon

        Expected: Create new canon + update existing mapping
        """
        # Arrange
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

        # Capture ingest call arguments
        ingest_calls = []

        def capture_ingest(new_canons, new_canon_packages, updated_canon_packages):
            ingest_calls.append(
                (new_canons, new_canon_packages, updated_canon_packages)
            )

        mock_db.ingest.side_effect = capture_ingest

        # Act
        with patch.dict("os.environ", {"LOAD": "true", "TEST": "false"}):
            main(self.mock_config, mock_db)

        # Assert
        self.assertEqual(len(ingest_calls), 1, "Should call ingest exactly once")

        new_canons, new_canon_packages, updated_canon_packages = ingest_calls[0]

        # Verify canon creation
        self.assertEqual(len(new_canons), 1, "Should create exactly one new canon")
        self.assertEqual(len(new_canon_packages), 0, "Should not create new mappings")
        self.assertEqual(
            len(updated_canon_packages), 1, "Should update exactly one mapping"
        )

        created_canon = new_canons[0]
        self.assertEqual(
            created_canon.url_id, self.url1_id, "Canon should reference correct URL ID"
        )
        self.assertEqual(
            created_canon.name, self.canonical_url, "Canon name should be URL"
        )

        # Verify mapping update (should point to NEW canon, not old one)
        updated_mapping = updated_canon_packages[0]
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

    def test_case_2a_no_changes_needed(self):
        """
        Test Case 2a: URL has canon AND package already linked to that canon

        Expected: Do nothing (no changes)
        """
        # Arrange
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

        # Capture ingest call arguments
        ingest_calls = []

        def capture_ingest(new_canons, new_canon_packages, updated_canon_packages):
            ingest_calls.append(
                (new_canons, new_canon_packages, updated_canon_packages)
            )

        mock_db.ingest.side_effect = capture_ingest

        # Act
        with patch.dict("os.environ", {"LOAD": "true", "TEST": "false"}):
            main(self.mock_config, mock_db)

        # Assert - should call ingest with empty lists (no changes)
        self.assertEqual(len(ingest_calls), 1, "Should call ingest exactly once")

        new_canons, new_canon_packages, updated_canon_packages = ingest_calls[0]

        self.assertEqual(len(new_canons), 0, "Should not create any canons")
        self.assertEqual(len(new_canon_packages), 0, "Should not create any mappings")
        self.assertEqual(
            len(updated_canon_packages), 0, "Should not update any mappings"
        )

    def test_case_2b_update_existing_mapping(self):
        """
        Test Case 2b: URL has canon AND package linked to different canon

        Expected: Update mapping to correct canon
        """
        # Arrange
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

        # Capture ingest call arguments
        ingest_calls = []

        def capture_ingest(new_canons, new_canon_packages, updated_canon_packages):
            ingest_calls.append(
                (new_canons, new_canon_packages, updated_canon_packages)
            )

        mock_db.ingest.side_effect = capture_ingest

        # Act
        with patch.dict("os.environ", {"LOAD": "true", "TEST": "false"}):
            main(self.mock_config, mock_db)

        # Assert
        self.assertEqual(len(ingest_calls), 1, "Should call ingest exactly once")

        new_canons, new_canon_packages, updated_canon_packages = ingest_calls[0]

        # Should only update mapping, no new creations
        self.assertEqual(len(new_canons), 0, "Should not create any canons")
        self.assertEqual(
            len(new_canon_packages), 0, "Should not create any new mappings"
        )
        self.assertEqual(
            len(updated_canon_packages), 1, "Should update exactly one mapping"
        )

        # Verify mapping update points to correct canon
        updated_mapping = updated_canon_packages[0]
        self.assertEqual(
            updated_mapping[0], self.pkg1_id, "Should update correct package"
        )
        self.assertEqual(
            updated_mapping[1], self.canon1_id, "Should update to correct canon"
        )
        self.assertNotEqual(
            updated_mapping[1], self.canon2_id, "Should NOT point to wrong canon"
        )

    def test_case_2c_create_new_mapping(self):
        """
        Test Case 2c: URL has canon AND package has no mapping

        Expected: Create new mapping to existing canon
        """
        # Arrange
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

        # Capture ingest call arguments
        ingest_calls = []

        def capture_ingest(new_canons, new_canon_packages, updated_canon_packages):
            ingest_calls.append(
                (new_canons, new_canon_packages, updated_canon_packages)
            )

        mock_db.ingest.side_effect = capture_ingest

        # Act
        with patch.dict("os.environ", {"LOAD": "true", "TEST": "false"}):
            main(self.mock_config, mock_db)

        # Assert
        self.assertEqual(len(ingest_calls), 1, "Should call ingest exactly once")

        new_canons, new_canon_packages, updated_canon_packages = ingest_calls[0]

        # Should only create new mapping, no updates or new canons
        self.assertEqual(len(new_canons), 0, "Should not create any canons")
        self.assertEqual(
            len(new_canon_packages), 1, "Should create exactly one new mapping"
        )
        self.assertEqual(
            len(updated_canon_packages), 0, "Should not update any mappings"
        )

        # Verify mapping creation points to existing canon
        created_mapping = new_canon_packages[0]
        self.assertEqual(
            created_mapping.package_id, self.pkg1_id, "Should map correct package"
        )
        self.assertEqual(
            created_mapping.canon_id, self.canon1_id, "Should map to existing canon"
        )


if __name__ == "__main__":
    unittest.main()
