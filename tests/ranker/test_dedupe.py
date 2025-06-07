"""
Test the package deduplication functionality in the ranker.

This module tests the dedupe.main function which handles deduplication of packages
based on their homepage URLs, creating and managing canonical package representations.
"""

from datetime import datetime
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest

from core.models import URL, Canon, Package
from ranker.config import DedupeConfig
from ranker.dedupe import DedupeDB, main


@pytest.fixture
def ids():
    """Fixture providing consistent IDs for testing."""
    return {
        "homepage_url_type": uuid4(),
        "package_manager": uuid4(),
        "pkg1": uuid4(),
        "pkg2": uuid4(),
        "pkg3": uuid4(),
        "canon1": uuid4(),
        "canon2": uuid4(),
        "canon3": uuid4(),
        "url1": uuid4(),
        "url2": uuid4(),
        "url3": uuid4(),
    }


@pytest.fixture
def test_packages(ids):
    """Fixture providing test package objects."""
    return {
        "package1": Package(
            id=ids["pkg1"],
            name="package1",
            package_manager_id=ids["package_manager"],
            import_id="pkg1",
            derived_id="npm/package1",
            created_at=datetime.now(),
            updated_at=datetime.now(),
        ),
        "package2": Package(
            id=ids["pkg2"],
            name="package2",
            package_manager_id=ids["package_manager"],
            import_id="pkg2",
            derived_id="npm/package2",
            created_at=datetime.now(),
            updated_at=datetime.now(),
        ),
        "package3": Package(
            id=ids["pkg3"],
            name="package3",
            package_manager_id=ids["package_manager"],
            import_id="pkg3",
            derived_id="npm/package3",
            created_at=datetime.now(),
            updated_at=datetime.now(),
        ),
    }


@pytest.fixture
def test_urls(ids):
    """Fixture providing test URL objects."""
    canonical_url = "github.com/example/repo"
    non_canonical_url = "https://github.com/example/repo"
    different_url = "https://gitlab.com/example/repo"

    return {
        "canonical": URL(
            id=ids["url1"],
            url=canonical_url,
            url_type_id=ids["homepage_url_type"],
            created_at=datetime.now(),
            updated_at=datetime.now(),
        ),
        "non_canonical": URL(
            id=ids["url2"],
            url=non_canonical_url,
            url_type_id=ids["homepage_url_type"],
            created_at=datetime.now(),
            updated_at=datetime.now(),
        ),
        "different": URL(
            id=ids["url3"],
            url=different_url,
            url_type_id=ids["homepage_url_type"],
            created_at=datetime.now(),
            updated_at=datetime.now(),
        ),
    }


@pytest.fixture
def mock_dedupe_config(ids):
    """Fixture providing mock DedupeConfig."""
    config = MagicMock(spec=DedupeConfig)
    config.load = True
    config.homepage_url_type_id = ids["homepage_url_type"]
    return config


@pytest.fixture
def mock_db():
    """Fixture providing mock DedupeDB."""
    return MagicMock(spec=DedupeDB)


def capture_ingest_calls(mock_db):
    """Helper function to capture arguments passed to db.ingest."""
    ingest_calls = []

    def capture_ingest(new_canons, new_canon_packages, updated_canon_packages):
        ingest_calls.append((new_canons, new_canon_packages, updated_canon_packages))

    mock_db.ingest.side_effect = capture_ingest
    return ingest_calls


@pytest.mark.ranker
class TestDedupe:
    """Test the deduplication of packages - focused on different cases."""

    def test_case_2d_new_canon_new_mapping(
        self, ids, test_packages, test_urls, mock_dedupe_config, mock_db
    ):
        """
        Test Case 2d: URL has no canon AND package has no existing mapping

        Expected: Create new canon + create new mapping
        """
        # Arrange
        package = test_packages["package1"]
        homepage_url = test_urls["canonical"]

        # Current state: no canons exist for this URL, no package mapping exists
        mock_db.get_current_canons.return_value = {}  # URL has no canon
        mock_db.get_current_canon_packages.return_value = {}  # Package has no mapping
        mock_db.get_packages_with_homepages.return_value = [(package, homepage_url)]

        ingest_calls = capture_ingest_calls(mock_db)

        # Act
        with patch.dict("os.environ", {"LOAD": "true", "TEST": "false"}):
            main(mock_dedupe_config, mock_db)

        # Assert
        assert len(ingest_calls) == 1, "Should call ingest exactly once"

        new_canons, new_canon_packages, updated_canon_packages = ingest_calls[0]

        # Verify canon creation
        assert len(new_canons) == 1, "Should create exactly one new canon"
        assert len(new_canon_packages) == 1, "Should create exactly one new mapping"
        assert len(updated_canon_packages) == 0, "Should not update any mappings"

        created_canon = new_canons[0]
        assert (
            created_canon.url_id == ids["url1"]
        ), "Canon should reference correct URL ID"
        assert created_canon.name == homepage_url.url, "Canon name should be URL"

        # Verify mapping creation
        created_mapping = new_canon_packages[0]
        assert created_mapping.package_id == ids["pkg1"], "Should map correct package"
        assert created_mapping.canon_id == created_canon.id, "Should map to new canon"

    def test_case_2d_new_canon_update_mapping(
        self, ids, test_packages, test_urls, mock_dedupe_config, mock_db
    ):
        """
        Test Case 2d: URL has no canon AND package has existing mapping to different
        canon

        Expected: Create new canon + update existing mapping
        """
        # Arrange
        package = test_packages["package1"]
        homepage_url = test_urls["canonical"]

        # Create existing canon for different URL
        existing_canon = Canon(
            id=ids["canon2"],
            url_id=ids["url2"],  # Different URL
            name="old-canon",
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )

        # Current state: no canon for this URL, but package is mapped to different canon
        mock_db.get_current_canons.return_value = {ids["url2"]: existing_canon}
        mock_db.get_current_canon_packages.return_value = {
            ids["pkg1"]: {"id": uuid4(), "canon_id": existing_canon.id}
        }
        mock_db.get_packages_with_homepages.return_value = [(package, homepage_url)]

        ingest_calls = capture_ingest_calls(mock_db)

        # Act
        with patch.dict("os.environ", {"LOAD": "true", "TEST": "false"}):
            main(mock_dedupe_config, mock_db)

        # Assert
        assert len(ingest_calls) == 1, "Should call ingest exactly once"

        new_canons, new_canon_packages, updated_canon_packages = ingest_calls[0]

        # Verify canon creation
        assert len(new_canons) == 1, "Should create exactly one new canon"
        assert len(new_canon_packages) == 0, "Should not create new mappings"
        assert len(updated_canon_packages) == 1, "Should update exactly one mapping"

        created_canon = new_canons[0]
        assert (
            created_canon.url_id == ids["url1"]
        ), "Canon should reference correct URL ID"
        assert created_canon.name == homepage_url.url, "Canon name should be URL"

        # Verify mapping update (should point to NEW canon, not old one)
        updated_mapping = updated_canon_packages[0]
        assert "id" in updated_mapping, "Update should include canon package ID"
        assert (
            updated_mapping["canon_id"] == created_canon.id
        ), "Should update to NEW canon"
        assert (
            updated_mapping["canon_id"] != ids["canon2"]
        ), "Should NOT point to old canon"
        assert "updated_at" in updated_mapping, "Update should include timestamp"

    def test_case_2a_no_changes_needed(
        self, ids, test_packages, test_urls, mock_dedupe_config, mock_db
    ):
        """
        Test Case 2a: URL has canon AND package already linked to that canon

        Expected: Do nothing (no changes)
        """
        # Arrange
        package = test_packages["package1"]
        homepage_url = test_urls["canonical"]

        existing_canon = Canon(
            id=ids["canon1"],
            url_id=ids["url1"],
            name="existing-canon",
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )

        # Current state: URL has canon, package linked to that same canon
        mock_db.get_current_canons.return_value = {ids["url1"]: existing_canon}
        mock_db.get_current_canon_packages.return_value = {
            ids["pkg1"]: {"id": uuid4(), "canon_id": ids["canon1"]}
        }
        mock_db.get_packages_with_homepages.return_value = [(package, homepage_url)]

        ingest_calls = capture_ingest_calls(mock_db)

        # Act
        with patch.dict("os.environ", {"LOAD": "true", "TEST": "false"}):
            main(mock_dedupe_config, mock_db)

        # Assert - should call ingest with empty lists (no changes)
        assert len(ingest_calls) == 1, "Should call ingest exactly once"

        new_canons, new_canon_packages, updated_canon_packages = ingest_calls[0]

        assert len(new_canons) == 0, "Should not create any canons"
        assert len(new_canon_packages) == 0, "Should not create any mappings"
        assert len(updated_canon_packages) == 0, "Should not update any mappings"

    def test_case_2b_update_existing_mapping(
        self, ids, test_packages, test_urls, mock_dedupe_config, mock_db
    ):
        """
        Test Case 2b: URL has canon AND package linked to different canon

        Expected: Update mapping to correct canon
        """
        # Arrange
        package = test_packages["package1"]
        homepage_url = test_urls["canonical"]

        correct_canon = Canon(
            id=ids["canon1"],
            url_id=ids["url1"],  # This URL's canon
            name="correct-canon",
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )

        wrong_canon = Canon(
            id=ids["canon2"],
            url_id=ids["url2"],  # Different URL's canon
            name="wrong-canon",
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )

        # Current state: URL has canon, but package linked to wrong canon
        mock_db.get_current_canons.return_value = {
            ids["url1"]: correct_canon,
            ids["url2"]: wrong_canon,
        }
        mock_db.get_current_canon_packages.return_value = {
            ids["pkg1"]: {
                "id": uuid4(),
                "canon_id": ids["canon2"],
            }  # Linked to wrong canon
        }
        mock_db.get_packages_with_homepages.return_value = [(package, homepage_url)]

        ingest_calls = capture_ingest_calls(mock_db)

        # Act
        with patch.dict("os.environ", {"LOAD": "true", "TEST": "false"}):
            main(mock_dedupe_config, mock_db)

        # Assert
        assert len(ingest_calls) == 1, "Should call ingest exactly once"

        new_canons, new_canon_packages, updated_canon_packages = ingest_calls[0]

        # Should only update mapping, no new creations
        assert len(new_canons) == 0, "Should not create any canons"
        assert len(new_canon_packages) == 0, "Should not create any new mappings"
        assert len(updated_canon_packages) == 1, "Should update exactly one mapping"

        # Verify mapping update points to correct canon
        updated_mapping = updated_canon_packages[0]
        assert "id" in updated_mapping, "Update should include canon package ID"
        assert (
            updated_mapping["canon_id"] == ids["canon1"]
        ), "Should update to correct canon"
        assert (
            updated_mapping["canon_id"] != ids["canon2"]
        ), "Should NOT point to wrong canon"
        assert "updated_at" in updated_mapping, "Update should include timestamp"

    def test_case_2c_create_new_mapping(
        self, ids, test_packages, test_urls, mock_dedupe_config, mock_db
    ):
        """
        Test Case 2c: URL has canon AND package has no mapping

        Expected: Create new mapping to existing canon
        """
        # Arrange
        package = test_packages["package1"]
        homepage_url = test_urls["canonical"]

        existing_canon = Canon(
            id=ids["canon1"],
            url_id=ids["url1"],
            name="existing-canon",
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )

        # Current state: URL has canon, but package has no mapping
        mock_db.get_current_canons.return_value = {ids["url1"]: existing_canon}
        mock_db.get_current_canon_packages.return_value = {}  # Package not linked
        mock_db.get_packages_with_homepages.return_value = [(package, homepage_url)]

        ingest_calls = capture_ingest_calls(mock_db)

        # Act
        with patch.dict("os.environ", {"LOAD": "true", "TEST": "false"}):
            main(mock_dedupe_config, mock_db)

        # Assert
        assert len(ingest_calls) == 1, "Should call ingest exactly once"

        new_canons, new_canon_packages, updated_canon_packages = ingest_calls[0]

        # Should only create new mapping, no updates or new canons
        assert len(new_canons) == 0, "Should not create any canons"
        assert len(new_canon_packages) == 1, "Should create exactly one new mapping"
        assert len(updated_canon_packages) == 0, "Should not update any mappings"

        # Verify mapping creation points to existing canon
        created_mapping = new_canon_packages[0]
        assert created_mapping.package_id == ids["pkg1"], "Should map correct package"
        assert created_mapping.canon_id == ids["canon1"], "Should map to existing canon"

    def test_multiple_packages_same_homepage_creates_single_canon(
        self, ids, test_packages, test_urls, mock_dedupe_config, mock_db
    ):
        """
        Test deduplication: Multiple packages with same homepage URL should create only
        one canon

        This tests the core deduplication logic where:
        - Package 1 points to URL X (no existing canon)
        - Package 2 also points to URL X
        - Should create only ONE canon for URL X
        - Both packages should be linked to the same canon
        """
        # Arrange
        package1 = test_packages["package1"]
        package2 = test_packages["package2"]
        shared_homepage_url = test_urls["canonical"]

        # Current state: no canons exist for this URL, no package mappings exist
        mock_db.get_current_canons.return_value = {}  # URL has no canon
        mock_db.get_current_canon_packages.return_value = {}  # No mappings
        mock_db.get_packages_with_homepages.return_value = [
            (package1, shared_homepage_url),  # Both packages point to same URL
            (package2, shared_homepage_url),
        ]

        ingest_calls = capture_ingest_calls(mock_db)

        # Act
        with patch.dict("os.environ", {"LOAD": "true", "TEST": "false"}):
            main(mock_dedupe_config, mock_db)

        # Assert
        assert len(ingest_calls) == 1, "Should call ingest exactly once"

        new_canons, new_canon_packages, updated_canon_packages = ingest_calls[0]

        # Should create only ONE canon for the shared URL
        assert len(new_canons) == 1, "Should create exactly one canon for shared URL"
        assert len(new_canon_packages) == 2, "Should create mappings for both packages"
        assert len(updated_canon_packages) == 0, "Should not update any mappings"

        # Verify single canon creation
        created_canon = new_canons[0]
        assert created_canon.url_id == ids["url1"], "Canon should reference shared URL"

        # Verify both packages map to the same canon
        canon_ids = {mapping.canon_id for mapping in new_canon_packages}
        assert len(canon_ids) == 1, "Both packages should map to same canon"
        assert (
            canon_ids.pop() == created_canon.id
        ), "Both should map to the created canon"

        # Verify package IDs
        package_ids = {mapping.package_id for mapping in new_canon_packages}
        assert package_ids == {ids["pkg1"], ids["pkg2"]}, "Should map both packages"

    def test_skip_when_load_disabled(self, mock_dedupe_config, mock_db):
        """
        Test that no processing occurs when load is disabled

        Expected: db.ingest should not be called
        """
        # Arrange
        mock_dedupe_config.load = False

        # Act
        with patch.dict("os.environ", {"LOAD": "false", "TEST": "false"}):
            main(mock_dedupe_config, mock_db)

        # Assert
        mock_db.ingest.assert_not_called()
