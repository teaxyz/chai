"""
Unit tests for the CratesTransformer class.

These tests verify the transformation logic from raw data to database models:
1. Package data transformation
2. Version data transformation
3. Dependency relationship transformation
4. User data transformation
5. URL data transformation

Each test uses a mock CSV reader to simulate data input and verifies
the correct transformation of that data into the expected format.

The test data comes from a real row from the crates.io database dump.
"""

import pytest

from package_managers.crates.structs import DependencyType
from package_managers.crates.transformer import CratesTransformer


@pytest.mark.transformer
class TestTransformer:
    """Tests for the CratesTransformer class"""

    @pytest.fixture
    def transformer(self, url_types, user_types):
        """Create a transformer instance with mocked dependencies"""
        return CratesTransformer(url_types=url_types, user_types=user_types)

    def test_packages_transform(self, transformer, mock_csv_reader):
        """
        Test package data transformation.

        Verifies:
        - Basic field mapping (id -> import_id, etc.)
        - Handling of optional fields (readme)
        - Data type conversions
        """
        test_data = {
            "id": "123",
            "name": "serde",
            "readme": "# Serde\nA serialization framework",
        }

        transformer._read_csv_rows = mock_csv_reader(test_data)

        packages = list(transformer.packages())
        assert len(packages) == 1

        package = packages[0]
        assert package["name"] == "serde"
        assert package["import_id"] == "123"
        assert package["readme"] == "# Serde\nA serialization framework"

    def test_versions_transform(self, transformer, mock_csv_reader):
        """
        Test version data transformation.

        Verifies:
        - Field mapping from crate fields to version fields
        - Type conversions (size to int, etc.)
        - Handling of timestamps
        - Processing of optional fields
        """
        test_data = {
            "crate_id": "123",
            "num": "1.0.0",
            "id": "456",
            "crate_size": "1000",
            "created_at": "2023-01-01T00:00:00Z",
            "license": "MIT",
            "downloads": "5000",
            "checksum": "abc123",
        }

        transformer._read_csv_rows = mock_csv_reader(test_data)

        versions = list(transformer.versions())
        assert len(versions) == 1

        version = versions[0]
        assert version["crate_id"] == "123"
        assert version["version"] == "1.0.0"
        assert version["import_id"] == "456"
        assert version["size"] == 1000
        assert version["published_at"] == "2023-01-01T00:00:00Z"
        assert version["license"] == "MIT"
        assert version["downloads"] == 5000
        assert version["checksum"] == "abc123"

    def test_dependencies_transform(self, transformer, mock_csv_reader):
        """
        Test dependency data transformation.

        Verifies:
        - Correct mapping of dependency fields
        - Handling of dependency types
        - Processing of version requirements
        """
        test_data = {
            "version_id": "456",
            "crate_id": "789",
            "req": "^1.0",
            "kind": "0",  # normal dependency
        }

        transformer._read_csv_rows = mock_csv_reader(test_data)

        dependencies = list(transformer.dependencies())
        assert len(dependencies) == 1

        dependency = dependencies[0]
        assert dependency["version_id"] == "456"
        assert dependency["crate_id"] == "789"
        assert dependency["semver_range"] == "^1.0"
        assert dependency["dependency_type"] == DependencyType(0)

    def test_users_transform(self, transformer, mock_csv_reader):
        """
        Test user data transformation.

        Verifies:
        - Mapping of GitHub login to username
        - Correct source assignment
        - Import ID handling
        """
        test_data = {"gh_login": "alice", "id": "user123"}

        transformer._read_csv_rows = mock_csv_reader(test_data)

        users = list(transformer.users())
        assert len(users) == 1

        user = users[0]
        assert user["import_id"] == "user123"
        assert user["username"] == "alice"
        assert user["source_id"] == transformer.user_types.github

    def test_package_urls_transform(self, transformer, mock_csv_reader):
        """
        Test package URLs transformation.

        Verifies:
        - Creation of separate URL entries for each type
        - Correct URL type assignment
        - Handling of missing URLs
        """
        test_data = {
            "id": "123",
            "homepage": "https://serde.rs",
            "repository": "https://github.com/serde-rs/serde",
            "documentation": "https://docs.rs/serde",
        }

        transformer._read_csv_rows = mock_csv_reader(test_data)

        urls = list(transformer.package_urls())
        assert len(urls) == 3  # One for each URL type

        # Check homepage URL
        homepage = next(
            url for url in urls if url["url_type_id"] == transformer.url_types.homepage
        )
        assert homepage["import_id"] == "123"
        assert homepage["url"] == "https://serde.rs"

        # Check repository URL
        repo = next(
            url
            for url in urls
            if url["url_type_id"] == transformer.url_types.repository
        )
        assert repo["import_id"] == "123"
        assert repo["url"] == "https://github.com/serde-rs/serde"

        # Check documentation URL
        docs = next(
            url
            for url in urls
            if url["url_type_id"] == transformer.url_types.documentation
        )
        assert docs["import_id"] == "123"
        assert docs["url"] == "https://docs.rs/serde"

    def test_user_versions_transform(self, transformer, mock_csv_reader):
        """
        Test user versions data transformation.

        Verifies:
        - Mapping of version publishing data
        - Correct handling of user associations
        """
        test_data = {"id": "version123", "published_by": "user456"}

        transformer._read_csv_rows = mock_csv_reader(test_data)

        user_versions = list(transformer.user_versions())
        assert len(user_versions) == 1

        user_version = user_versions[0]
        assert user_version["version_id"] == "version123"
        assert user_version["published_by"] == "user456"

    def test_urls_transform(self, transformer, mock_csv_reader):
        """
        Test URLs transformation.

        Verifies:
        - Extraction of URLs from package data
        - Correct type assignment for each URL
        - Handling of all URL types
        """
        test_data = {
            "homepage": "https://serde.rs",
            "repository": "https://github.com/serde-rs/serde",
            "documentation": "https://docs.rs/serde",
        }

        transformer._read_csv_rows = mock_csv_reader(test_data)

        urls = list(transformer.urls())
        assert len(urls) == 3  # One for each URL type

        # Check that each URL type is present
        url_types_found = {url["url_type_id"] for url in urls}
        assert transformer.url_types.homepage in url_types_found
        assert transformer.url_types.repository in url_types_found
        assert transformer.url_types.documentation in url_types_found

        # Check specific URLs
        homepage = next(
            url for url in urls if url["url_type_id"] == transformer.url_types.homepage
        )
        assert homepage["url"] == "https://serde.rs"

        repo = next(
            url
            for url in urls
            if url["url_type_id"] == transformer.url_types.repository
        )
        assert repo["url"] == "https://github.com/serde-rs/serde"

        docs = next(
            url
            for url in urls
            if url["url_type_id"] == transformer.url_types.documentation
        )
        assert docs["url"] == "https://docs.rs/serde"
