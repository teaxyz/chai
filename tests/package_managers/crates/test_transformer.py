#!/usr/bin/env pkgx uv run
"""
Comprehensive tests for package_managers/crates/transformer.py module.

Tests cover CratesTransformer class initialization, CSV parsing,
data transformation, and error handling.
"""

import csv
import os
import tempfile
from io import StringIO
from unittest.mock import MagicMock, Mock, mock_open, patch

import pytest

from package_managers.crates.transformer import CratesTransformer
from package_managers.crates.structs import (
    Crate,
    CrateDependency,
    CrateLatestVersion,
    CrateUser,
    DependencyType,
)


class TestCratesTransformer:
    """Test cases for the CratesTransformer class."""

    def test_transformer_init(self, mock_config):
        """Test CratesTransformer initialization."""
        transformer = CratesTransformer(mock_config)
        
        assert transformer.name == "crates"
        assert transformer.config == mock_config
        assert transformer.crates == {}
        
        # Check expected files configuration
        expected_files = {
            "crates": "crates.csv",
            "latest_versions": "default_versions.csv",
            "versions": "versions.csv",
            "dependencies": "dependencies.csv",
            "users": "users.csv",
            "teams": "teams.csv",
        }
        assert transformer.files == expected_files

    @patch("builtins.open", new_callable=mock_open)
    def test_open_csv_success(self, mock_file, mock_config):
        """Test _open_csv method with successful file reading."""
        csv_content = "id,name,value\n1,test,100\n2,example,200"
        mock_file.return_value.__enter__.return_value = StringIO(csv_content)
        
        transformer = CratesTransformer(mock_config)
        transformer.finder = Mock(return_value="/path/to/crates.csv")
        
        rows = list(transformer._open_csv("crates"))
        
        assert len(rows) == 2
        assert rows[0] == {"id": "1", "name": "test", "value": "100"}
        assert rows[1] == {"id": "2", "name": "example", "value": "200"}
        mock_file.assert_called_once_with("/path/to/crates.csv", newline="", encoding="utf-8")

    def test_open_csv_missing_file_key(self, mock_config):
        """Test _open_csv method with missing file key."""
        transformer = CratesTransformer(mock_config)
        
        with pytest.raises(KeyError, match="Missing nonexistent from self.files"):
            list(transformer._open_csv("nonexistent"))

    @patch("builtins.open", side_effect=FileNotFoundError("File not found"))
    def test_open_csv_file_not_found(self, mock_file, mock_config):
        """Test _open_csv method when file doesn't exist."""
        transformer = CratesTransformer(mock_config)
        transformer.finder = Mock(return_value="/path/to/missing.csv")
        
        with pytest.raises(FileNotFoundError, match="Missing /path/to/missing.csv file"):
            list(transformer._open_csv("crates"))

    @patch("builtins.open", side_effect=PermissionError("Access denied"))
    def test_open_csv_permission_error(self, mock_file, mock_config):
        """Test _open_csv method with permission error."""
        transformer = CratesTransformer(mock_config)
        transformer.finder = Mock(return_value="/path/to/restricted.csv")
        
        with pytest.raises(PermissionError):
            list(transformer._open_csv("crates"))

    @patch("core.utils.is_github_url")
    def test_parse_complete_workflow(self, mock_is_github, mock_config):
        """Test complete parse workflow with all CSV files."""
        mock_is_github.return_value = True
        
        # Mock CSV data
        crates_data = "id,name,readme,homepage,documentation,repository\n1,test-crate,README content,https://example.com,https://docs.example.com,https://github.com/user/repo"
        latest_versions_data = "crate_id,version_id\n1,100"
        versions_data = "id,crate_id,checksum,downloads,license,num,created_at,published_by\n100,1,abc123,5000,MIT,1.0.0,2023-01-01T00:00:00Z,1"
        users_data = "id,name,gh_login\n1,Test User,testuser"
        dependencies_data = "version_id,crate_id,kind,req\n100,2,0,^1.0"
        teams_data = "id,name\n1,Test Team"
        
        transformer = CratesTransformer(mock_config)
        
        # Mock the _open_csv method to return appropriate data for each file
        def mock_open_csv(file_name):
            data_map = {
                "crates": crates_data,
                "latest_versions": latest_versions_data,
                "versions": versions_data,
                "users": users_data,
                "dependencies": dependencies_data,
                "teams": teams_data,
            }
            csv_reader = csv.DictReader(StringIO(data_map[file_name]))
            return csv_reader
        
        transformer._open_csv = Mock(side_effect=mock_open_csv)
        transformer.canonicalize = Mock(side_effect=lambda x: x.replace("https://", ""))
        
        transformer.parse()
        
        # Verify crate was parsed correctly
        assert len(transformer.crates) == 1
        crate = transformer.crates[1]
        assert isinstance(crate, Crate)
        assert crate.name == "test-crate"
        assert crate.readme == "README content"
        assert crate.homepage == "example.com"
        assert crate.repository == "github.com/user/repo"
        assert crate.source == "github.com/user/repo"
        
        # Verify latest version was attached
        assert crate.latest_version is not None
        assert crate.latest_version.num == "1.0.0"
        assert crate.latest_version.license == "MIT"
        assert crate.latest_version.downloads == 5000

    def test_parse_crates_only(self, mock_config):
        """Test parsing only crates CSV."""
        crates_data = "id,name,readme,homepage,documentation,repository\n1,simple-crate,Simple README,,,\n2,another-crate,Another README,,,"
        
        transformer = CratesTransformer(mock_config)
        transformer._open_csv = Mock()
        transformer._open_csv.side_effect = lambda file_name: (
            csv.DictReader(StringIO(crates_data)) if file_name == "crates"
            else csv.DictReader(StringIO("crate_id,version_id\n")) if file_name == "latest_versions"
            else csv.DictReader(StringIO("id,name,gh_login\n")) if file_name == "users"
            else csv.DictReader(StringIO("id,crate_id,checksum,downloads,license,num,created_at,published_by\n"))
        )
        transformer.canonicalize = Mock(side_effect=lambda x: x or "")
        
        transformer.parse()
        
        assert len(transformer.crates) == 2
        assert transformer.crates[1].name == "simple-crate"
        assert transformer.crates[2].name == "another-crate"

    def test_load_latest_versions(self, mock_config):
        """Test _load_latest_versions method."""
        latest_versions_data = "crate_id,version_id\n1,100\n2,200\n3,300"
        
        transformer = CratesTransformer(mock_config)
        transformer._open_csv = Mock(return_value=csv.DictReader(StringIO(latest_versions_data)))
        
        latest_versions, latest_versions_map = transformer._load_latest_versions()
        
        assert latest_versions == {100, 200, 300}
        assert latest_versions_map == {100: 1, 200: 2, 300: 3}

    def test_load_latest_versions_empty(self, mock_config):
        """Test _load_latest_versions with empty data."""
        transformer = CratesTransformer(mock_config)
        transformer._open_csv = Mock(return_value=csv.DictReader(StringIO("crate_id,version_id\n")))
        
        latest_versions, latest_versions_map = transformer._load_latest_versions()
        
        assert latest_versions == set()
        assert latest_versions_map == {}

    def test_load_users(self, mock_config):
        """Test _load_users method."""
        users_data = "id,name,gh_login\n1,John Doe,johndoe\n2,Jane Smith,janesmith\n3,Team Bot,"
        
        transformer = CratesTransformer(mock_config)
        transformer._open_csv = Mock(return_value=csv.DictReader(StringIO(users_data)))
        
        users = transformer._load_users()
        
        assert len(users) == 3
        assert users[1].name == "John Doe"
        assert users[1].github_username == "johndoe"
        assert users[2].name == "Jane Smith"
        assert users[3].name == "Team Bot"
        assert users[3].github_username == ""

    def test_load_users_empty(self, mock_config):
        """Test _load_users with empty data."""
        transformer = CratesTransformer(mock_config)
        transformer._open_csv = Mock(return_value=csv.DictReader(StringIO("id,name,gh_login\n")))
        
        users = transformer._load_users()
        
        assert users == {}

    def test_parse_with_dependencies(self, mock_config):
        """Test parsing with dependency relationships."""
        # Setup test data
        crates_data = "id,name,readme,homepage,documentation,repository\n1,main-crate,Main README,,,\n2,dep-crate,Dep README,,,"
        latest_versions_data = "crate_id,version_id\n1,100\n2,200"
        versions_data = "id,crate_id,checksum,downloads,license,num,created_at,published_by\n100,1,abc123,1000,MIT,1.0.0,2023-01-01T00:00:00Z,\n200,2,def456,500,Apache-2.0,0.5.0,2023-01-01T00:00:00Z,"
        users_data = "id,name,gh_login\n"
        dependencies_data = "version_id,crate_id,kind,req\n100,2,0,^0.5\n100,2,1,^0.5"  # Same dependency with different kinds
        
        transformer = CratesTransformer(mock_config)
        
        def mock_open_csv(file_name):
            data_map = {
                "crates": crates_data,
                "latest_versions": latest_versions_data,
                "versions": versions_data,
                "users": users_data,
                "dependencies": dependencies_data,
                "teams": "",
            }
            return csv.DictReader(StringIO(data_map[file_name]))
        
        transformer._open_csv = Mock(side_effect=mock_open_csv)
        transformer.canonicalize = Mock(side_effect=lambda x: x or "")
        
        transformer.parse()
        
        # Verify dependency was added
        main_crate = transformer.crates[1]
        assert main_crate.latest_version is not None
        assert len(main_crate.latest_version.dependencies) == 2
        
        dep1, dep2 = main_crate.latest_version.dependencies
        assert dep1.dependency_id == 2
        assert dep1.dependency_type in [DependencyType.NORMAL, DependencyType.BUILD]
        assert dep1.semver_range == "^0.5"

    def test_parse_with_invalid_dependency_kind(self, mock_config):
        """Test parsing with invalid dependency kind."""
        crates_data = "id,name,readme,homepage,documentation,repository\n1,test-crate,README,,,"
        latest_versions_data = "crate_id,version_id\n1,100"
        versions_data = "id,crate_id,checksum,downloads,license,num,created_at,published_by\n100,1,abc123,1000,MIT,1.0.0,2023-01-01T00:00:00Z,"
        users_data = "id,name,gh_login\n"
        dependencies_data = "version_id,crate_id,kind,req\n100,2,999,^1.0"  # Invalid kind
        
        transformer = CratesTransformer(mock_config)
        
        def mock_open_csv(file_name):
            data_map = {
                "crates": crates_data,
                "latest_versions": latest_versions_data,
                "versions": versions_data,
                "users": users_data,
                "dependencies": dependencies_data,
                "teams": "",
            }
            return csv.DictReader(StringIO(data_map[file_name]))
        
        transformer._open_csv = Mock(side_effect=mock_open_csv)
        transformer.canonicalize = Mock(side_effect=lambda x: x or "")
        
        with pytest.raises(ValueError, match="Unknown dependency kind: 999"):
            transformer.parse()

    def test_parse_missing_crate_in_dependencies(self, mock_config):
        """Test parsing when dependency references missing crate."""
        crates_data = "id,name,readme,homepage,documentation,repository\n1,test-crate,README,,,"
        latest_versions_data = "crate_id,version_id\n1,100"
        versions_data = "id,crate_id,checksum,downloads,license,num,created_at,published_by\n100,999,abc123,1000,MIT,1.0.0,2023-01-01T00:00:00Z,"  # Wrong crate_id
        users_data = "id,name,gh_login\n"
        dependencies_data = "version_id,crate_id,kind,req\n"
        
        transformer = CratesTransformer(mock_config)
        
        def mock_open_csv(file_name):
            data_map = {
                "crates": crates_data,
                "latest_versions": latest_versions_data,
                "versions": versions_data,
                "users": users_data,
                "dependencies": dependencies_data,
                "teams": "",
            }
            return csv.DictReader(StringIO(data_map[file_name]))
        
        transformer._open_csv = Mock(side_effect=mock_open_csv)
        transformer.canonicalize = Mock(side_effect=lambda x: x or "")
        
        with pytest.raises(ValueError, match="Crate 999 not found in self.crates"):
            transformer.parse()

    def test_parse_non_latest_versions_ignored(self, mock_config):
        """Test that non-latest versions are ignored during parsing."""
        crates_data = "id,name,readme,homepage,documentation,repository\n1,test-crate,README,,,"
        latest_versions_data = "crate_id,version_id\n1,100"  # Only version 100 is latest
        versions_data = "id,crate_id,checksum,downloads,license,num,created_at,published_by\n100,1,abc123,1000,MIT,1.0.0,2023-01-01T00:00:00Z,\n101,1,def456,500,MIT,0.9.0,2023-01-01T00:00:00Z,"  # 101 is not latest
        users_data = "id,name,gh_login\n"
        dependencies_data = "version_id,crate_id,kind,req\n101,2,0,^1.0"  # Dependency for non-latest version
        
        transformer = CratesTransformer(mock_config)
        
        def mock_open_csv(file_name):
            data_map = {
                "crates": crates_data,
                "latest_versions": latest_versions_data,
                "versions": versions_data,
                "users": users_data,
                "dependencies": dependencies_data,
                "teams": "",
            }
            return csv.DictReader(StringIO(data_map[file_name]))
        
        transformer._open_csv = Mock(side_effect=mock_open_csv)
        transformer.canonicalize = Mock(side_effect=lambda x: x or "")
        
        transformer.parse()
        
        # Verify only latest version was processed
        crate = transformer.crates[1]
        assert crate.latest_version is not None
        assert crate.latest_version.num == "1.0.0"  # Latest version
        assert len(crate.latest_version.dependencies) == 0  # No deps for non-latest

    @patch("core.utils.is_github_url")
    def test_parse_with_github_source_detection(self, mock_is_github, mock_config):
        """Test GitHub URL source detection."""
        mock_is_github.side_effect = lambda url: "github.com" in url
        
        crates_data = "id,name,readme,homepage,documentation,repository\n1,github-crate,README,,,https://github.com/user/repo\n2,other-crate,README,,,https://gitlab.com/user/repo"
        
        transformer = CratesTransformer(mock_config)
        transformer._open_csv = Mock()
        transformer._open_csv.side_effect = lambda file_name: (
            csv.DictReader(StringIO(crates_data)) if file_name == "crates"
            else csv.DictReader(StringIO("crate_id,version_id\n"))
        )
        transformer.canonicalize = Mock(side_effect=lambda x: x.replace("https://", "") if x else "")
        
        transformer.parse()
        
        # GitHub repo should have source set
        github_crate = transformer.crates[1]
        assert github_crate.source == "github.com/user/repo"
        
        # Non-GitHub repo should not have source
        other_crate = transformer.crates[2]
        assert other_crate.source is None

    def test_parse_with_published_by_user(self, mock_config):
        """Test parsing with user who published the version."""
        crates_data = "id,name,readme,homepage,documentation,repository\n1,test-crate,README,,,"
        latest_versions_data = "crate_id,version_id\n1,100"
        versions_data = "id,crate_id,checksum,downloads,license,num,created_at,published_by\n100,1,abc123,1000,MIT,1.0.0,2023-01-01T00:00:00Z,5"
        users_data = "id,name,gh_login\n5,Publisher User,pubuser"
        dependencies_data = "version_id,crate_id,kind,req\n"
        
        transformer = CratesTransformer(mock_config)
        
        def mock_open_csv(file_name):
            data_map = {
                "crates": crates_data,
                "latest_versions": latest_versions_data,
                "versions": versions_data,
                "users": users_data,
                "dependencies": dependencies_data,
                "teams": "",
            }
            return csv.DictReader(StringIO(data_map[file_name]))
        
        transformer._open_csv = Mock(side_effect=mock_open_csv)
        transformer.canonicalize = Mock(side_effect=lambda x: x or "")
        
        transformer.parse()
        
        crate = transformer.crates[1]
        assert crate.latest_version is not None
        assert crate.latest_version.published_by is not None
        assert crate.latest_version.published_by.name == "Publisher User"
        assert crate.latest_version.published_by.github_username == "pubuser"

    def test_parse_empty_csv_files(self, mock_config):
        """Test parsing with empty CSV files."""
        transformer = CratesTransformer(mock_config)
        
        # Mock empty CSV files
        transformer._open_csv = Mock(return_value=csv.DictReader(StringIO("id,name\n")))
        transformer.canonicalize = Mock(side_effect=lambda x: x or "")
        
        transformer.parse()
        
        assert len(transformer.crates) == 0


# Integration tests
class TestCratesTransformerIntegration:
    """Integration tests for CratesTransformer."""

    def test_real_file_operations(self, mock_config):
        """Test transformer with actual file operations."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create test CSV files
            crates_file = os.path.join(temp_dir, "crates.csv")
            with open(crates_file, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(["id", "name", "readme", "homepage", "documentation", "repository"])
                writer.writerow(["1", "test-crate", "Test README", "", "", ""])
            
            latest_versions_file = os.path.join(temp_dir, "default_versions.csv")
            with open(latest_versions_file, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(["crate_id", "version_id"])
                writer.writerow(["1", "100"])
            
            versions_file = os.path.join(temp_dir, "versions.csv")
            with open(versions_file, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(["id", "crate_id", "checksum", "downloads", "license", "num", "created_at", "published_by"])
                writer.writerow(["100", "1", "abc123", "1000", "MIT", "1.0.0", "2023-01-01T00:00:00Z", ""])
            
            users_file = os.path.join(temp_dir, "users.csv")
            with open(users_file, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(["id", "name", "gh_login"])
            
            teams_file = os.path.join(temp_dir, "teams.csv")
            with open(teams_file, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(["id", "name"])
            
            dependencies_file = os.path.join(temp_dir, "dependencies.csv")
            with open(dependencies_file, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(["version_id", "crate_id", "kind", "req"])
            
            # Mock finder to return our test files
            transformer = CratesTransformer(mock_config)
            transformer.finder = Mock(side_effect=lambda file_name: {
                "crates.csv": crates_file,
                "default_versions.csv": latest_versions_file,
                "versions.csv": versions_file,
                "users.csv": users_file,
                "teams.csv": teams_file,
                "dependencies.csv": dependencies_file,
            }[file_name])
            transformer.canonicalize = Mock(side_effect=lambda x: x or "")
            
            transformer.parse()
            
            assert len(transformer.crates) == 1
            crate = transformer.crates[1]
            assert crate.name == "test-crate"
            assert crate.latest_version is not None
            assert crate.latest_version.num == "1.0.0"

    def test_error_propagation(self, mock_config):
        """Test that errors in CSV processing are properly propagated."""
        transformer = CratesTransformer(mock_config)
        transformer._open_csv = Mock(side_effect=Exception("CSV processing error"))
        
        with pytest.raises(Exception, match="CSV processing error"):
            transformer.parse()