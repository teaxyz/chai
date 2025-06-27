#!/usr/bin/env pkgx uv run
"""
Comprehensive tests for core/transformer.py module.

Tests cover Transformer class initialization, file operations,
URL canonicalization, and name guessing functionality.
"""

import os
import tempfile
import uuid
from unittest.mock import MagicMock, Mock, patch, mock_open

import pytest

from core.transformer import Transformer


class TestTransformer:
    """Test cases for the Transformer class."""

    def test_transformer_init(self):
        """Test Transformer initialization."""
        transformer = Transformer("test-package-manager")
        
        assert transformer.name == "test-package-manager"
        assert transformer.input == "data/test-package-manager/latest"
        assert transformer.logger is not None
        
        # Check default files structure
        expected_files = {
            "projects": "",
            "versions": "",
            "dependencies": "",
            "users": "",
            "urls": ""
        }
        assert transformer.files == expected_files
        assert transformer.url_types == {}

    def test_transformer_init_different_names(self):
        """Test Transformer initialization with different names."""
        names = ["crates", "homebrew", "debian", "pkgx"]
        
        for name in names:
            transformer = Transformer(name)
            assert transformer.name == name
            assert transformer.input == f"data/{name}/latest"

    @patch("os.walk")
    @patch("os.path.realpath")
    def test_finder_file_found(self, mock_realpath, mock_walk):
        """Test finder method when file is found."""
        mock_realpath.return_value = "/real/path/data/test/latest"
        mock_walk.return_value = [
            ("/real/path/data/test/latest", ["subdir"], ["file1.txt", "target.csv"]),
            ("/real/path/data/test/latest/subdir", [], ["file2.txt"])
        ]
        
        transformer = Transformer("test")
        result = transformer.finder("target.csv")
        
        assert result == "/real/path/data/test/latest/target.csv"
        mock_realpath.assert_called_once_with("data/test/latest")

    @patch("os.walk")
    @patch("os.path.realpath")
    def test_finder_file_in_subdirectory(self, mock_realpath, mock_walk):
        """Test finder method when file is in subdirectory."""
        mock_realpath.return_value = "/real/path/data/test/latest"
        mock_walk.return_value = [
            ("/real/path/data/test/latest", ["subdir"], ["file1.txt"]),
            ("/real/path/data/test/latest/subdir", [], ["target.csv", "file2.txt"])
        ]
        
        transformer = Transformer("test")
        result = transformer.finder("target.csv")
        
        assert result == "/real/path/data/test/latest/subdir/target.csv"

    @patch("os.walk")
    @patch("os.path.realpath")
    def test_finder_file_not_found(self, mock_realpath, mock_walk):
        """Test finder method when file is not found."""
        mock_realpath.return_value = "/real/path/data/test/latest"
        mock_walk.return_value = [
            ("/real/path/data/test/latest", [], ["file1.txt", "file2.txt"]),
        ]
        
        transformer = Transformer("test")
        
        with pytest.raises(FileNotFoundError, match="Missing nonexistent.csv file"):
            transformer.finder("nonexistent.csv")

    @patch("os.walk")
    @patch("os.path.realpath")
    def test_finder_empty_directory(self, mock_realpath, mock_walk):
        """Test finder method with empty directory."""
        mock_realpath.return_value = "/real/path/data/test/latest"
        mock_walk.return_value = [
            ("/real/path/data/test/latest", [], []),
        ]
        
        transformer = Transformer("test")
        
        with pytest.raises(FileNotFoundError):
            transformer.finder("any-file.csv")

    @patch("os.walk")
    @patch("os.path.realpath")
    def test_finder_multiple_files_same_name(self, mock_realpath, mock_walk):
        """Test finder method when multiple files have the same name."""
        mock_realpath.return_value = "/real/path/data/test/latest"
        mock_walk.return_value = [
            ("/real/path/data/test/latest", ["dir1", "dir2"], []),
            ("/real/path/data/test/latest/dir1", [], ["target.csv"]),
            ("/real/path/data/test/latest/dir2", [], ["target.csv"])
        ]
        
        transformer = Transformer("test")
        result = transformer.finder("target.csv")
        
        # Should return the first match found
        assert result == "/real/path/data/test/latest/dir1/target.csv"

    @patch("builtins.open", new_callable=mock_open, read_data="file content")
    def test_open_file_success(self, mock_file):
        """Test open method successfully reading file."""
        transformer = Transformer("test")
        
        with patch.object(transformer, 'finder', return_value="/path/to/file.txt"):
            result = transformer.open("file.txt")
        
        assert result == "file content"
        mock_file.assert_called_once_with("/path/to/file.txt")

    @patch("builtins.open", new_callable=mock_open, read_data="")
    def test_open_empty_file(self, mock_file):
        """Test open method with empty file."""
        transformer = Transformer("test")
        
        with patch.object(transformer, 'finder', return_value="/path/to/empty.txt"):
            result = transformer.open("empty.txt")
        
        assert result == ""

    @patch("builtins.open", side_effect=IOError("Permission denied"))
    def test_open_file_io_error(self, mock_file):
        """Test open method with file I/O error."""
        transformer = Transformer("test")
        
        with patch.object(transformer, 'finder', return_value="/path/to/file.txt"):
            with pytest.raises(IOError, match="Permission denied"):
                transformer.open("file.txt")

    def test_open_file_not_found_by_finder(self):
        """Test open method when finder raises FileNotFoundError."""
        transformer = Transformer("test")
        
        with patch.object(transformer, 'finder', side_effect=FileNotFoundError("File not found")):
            with pytest.raises(FileNotFoundError):
                transformer.open("nonexistent.txt")

    @patch("builtins.open", new_callable=mock_open, read_data="multi\nline\ncontent")
    def test_open_multiline_file(self, mock_file):
        """Test open method with multiline file content."""
        transformer = Transformer("test")
        
        with patch.object(transformer, 'finder', return_value="/path/to/multiline.txt"):
            result = transformer.open("multiline.txt")
        
        assert result == "multi\nline\ncontent"

    @patch("permalint.normalize_url")
    def test_canonicalize_success(self, mock_normalize):
        """Test canonicalize method successfully normalizing URL."""
        mock_normalize.return_value = "github.com/user/repo"
        
        transformer = Transformer("test")
        result = transformer.canonicalize("https://github.com/user/repo.git")
        
        mock_normalize.assert_called_once_with("https://github.com/user/repo.git")
        assert result == "github.com/user/repo"

    @patch("permalint.normalize_url")
    def test_canonicalize_various_urls(self, mock_normalize):
        """Test canonicalize method with various URL formats."""
        test_cases = [
            ("https://github.com/user/repo", "github.com/user/repo"),
            ("https://gitlab.com/user/project", "gitlab.com/user/project"),
            ("https://bitbucket.org/user/repo", "bitbucket.org/user/repo"),
            ("https://example.com/path", "example.com/path")
        ]
        
        transformer = Transformer("test")
        
        for input_url, expected_output in test_cases:
            mock_normalize.return_value = expected_output
            result = transformer.canonicalize(input_url)
            assert result == expected_output

    @patch("permalint.normalize_url")
    def test_canonicalize_error_handling(self, mock_normalize):
        """Test canonicalize method error handling."""
        mock_normalize.side_effect = Exception("Invalid URL")
        
        transformer = Transformer("test")
        
        with pytest.raises(Exception, match="Invalid URL"):
            transformer.canonicalize("invalid-url")

    @patch("permalint.possible_names")
    def test_guess_with_results(self, mock_possible_names):
        """Test guess method returning package names."""
        mock_possible_names.return_value = ["package1", "package2", "package3"]
        mock_db = Mock()
        mock_db.search_names.return_value = ["https://example.com/package1", "https://example.com/package2"]
        
        transformer = Transformer("test")
        package_managers = [uuid.uuid4(), uuid.uuid4()]
        
        result = transformer.guess(mock_db, "https://github.com/user/repo", package_managers)
        
        mock_possible_names.assert_called_once_with("https://github.com/user/repo")
        mock_db.search_names.assert_called_once_with(["package1", "package2", "package3"], package_managers)
        assert result == ["https://example.com/package1", "https://example.com/package2"]

    @patch("permalint.possible_names")
    def test_guess_no_names_found(self, mock_possible_names):
        """Test guess method when no possible names found."""
        mock_possible_names.return_value = []
        mock_db = Mock()
        mock_db.search_names.return_value = []
        
        transformer = Transformer("test")
        
        result = transformer.guess(mock_db, "https://unknown-site.com/repo", [])
        
        assert result == []
        mock_db.search_names.assert_called_once_with([], [])

    @patch("permalint.possible_names")
    def test_guess_db_returns_empty(self, mock_possible_names):
        """Test guess method when database returns empty results."""
        mock_possible_names.return_value = ["package1", "package2"]
        mock_db = Mock()
        mock_db.search_names.return_value = []
        
        transformer = Transformer("test")
        
        result = transformer.guess(mock_db, "https://github.com/user/repo", [uuid.uuid4()])
        
        assert result == []

    @patch("permalint.possible_names")
    def test_guess_with_single_package_manager(self, mock_possible_names):
        """Test guess method with single package manager."""
        mock_possible_names.return_value = ["package1"]
        mock_db = Mock()
        mock_db.search_names.return_value = ["https://homepage.com"]
        
        transformer = Transformer("test")
        package_manager_id = uuid4()
        
        result = transformer.guess(mock_db, "https://github.com/user/repo", [package_manager_id])
        
        mock_db.search_names.assert_called_once_with(["package1"], [package_manager_id])
        assert result == ["https://homepage.com"]

    @patch("permalint.possible_names")
    def test_guess_error_handling(self, mock_possible_names):
        """Test guess method error handling."""
        mock_possible_names.side_effect = Exception("URL parsing error")
        mock_db = Mock()
        
        transformer = Transformer("test")
        
        with pytest.raises(Exception, match="URL parsing error"):
            transformer.guess(mock_db, "invalid-url", [])


# Integration tests
class TestTransformerIntegration:
    """Integration tests for Transformer class."""

    def test_full_file_workflow(self):
        """Test complete workflow from finder to open."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create test directory structure
            test_dir = os.path.join(temp_dir, "test-data")
            subdir = os.path.join(test_dir, "subdir")
            os.makedirs(subdir)
            
            # Create test file
            test_file = os.path.join(subdir, "data.csv")
            test_content = "name,version\npackage1,1.0.0\npackage2,2.0.0"
            
            with open(test_file, 'w') as f:
                f.write(test_content)
            
            # Test transformer
            transformer = Transformer("test")
            transformer.input = test_dir
            
            # Find and open file
            found_path = transformer.finder("data.csv")
            content = transformer.open("data.csv")
            
            assert found_path == test_file
            assert content == test_content

    @patch("permalint.normalize_url")
    @patch("permalint.possible_names")
    def test_canonicalize_then_guess_workflow(self, mock_possible_names, mock_normalize):
        """Test workflow combining canonicalize and guess methods."""
        # Setup mocks
        mock_normalize.return_value = "github.com/user/repo"
        mock_possible_names.return_value = ["repo", "user-repo"]
        
        mock_db = Mock()
        mock_db.search_names.return_value = ["https://repo-homepage.com"]
        
        transformer = Transformer("test")
        
        # First canonicalize URL
        canonical_url = transformer.canonicalize("https://github.com/user/repo.git")
        
        # Then guess package names
        results = transformer.guess(mock_db, canonical_url, [uuid4()])
        
        assert canonical_url == "github.com/user/repo"
        assert results == ["https://repo-homepage.com"]
        
        # Verify both functions were called
        mock_normalize.assert_called_once()
        mock_possible_names.assert_called_once_with("github.com/user/repo")

    def test_files_dict_modification(self):
        """Test that files dictionary can be modified after initialization."""
        transformer = Transformer("test")
        
        # Modify files mapping
        transformer.files["projects"] = "projects.csv"
        transformer.files["custom_file"] = "custom.json"
        
        assert transformer.files["projects"] == "projects.csv"
        assert transformer.files["custom_file"] == "custom.json"
        assert "versions" in transformer.files  # Other keys should remain

    def test_url_types_dict_modification(self):
        """Test that url_types dictionary can be modified after initialization."""
        transformer = Transformer("test")
        
        # Add URL types
        homepage_id = uuid4()
        repository_id = uuid4()
        
        transformer.url_types["homepage"] = homepage_id
        transformer.url_types["repository"] = repository_id
        
        assert transformer.url_types["homepage"] == homepage_id
        assert transformer.url_types["repository"] == repository_id


# Error handling and edge cases
class TestTransformerEdgeCases:
    """Test edge cases and error conditions."""

    def test_transformer_with_special_characters_in_name(self):
        """Test Transformer with special characters in name."""
        names_with_special_chars = [
            "package-manager",
            "package_manager",
            "package.manager",
            "package@manager"
        ]
        
        for name in names_with_special_chars:
            transformer = Transformer(name)
            assert transformer.name == name
            assert transformer.input == f"data/{name}/latest"

    @patch("os.walk")
    @patch("os.path.realpath")
    def test_finder_with_permission_error(self, mock_realpath, mock_walk):
        """Test finder method with permission error during directory walk."""
        mock_realpath.return_value = "/real/path/data/test/latest"
        mock_walk.side_effect = PermissionError("Permission denied")
        
        transformer = Transformer("test")
        
        with pytest.raises(PermissionError):
            transformer.finder("any-file.txt")

    @patch("builtins.open", new_callable=mock_open, read_data="content with unicode: 🚀 émojis")
    def test_open_file_with_unicode(self, mock_file):
        """Test open method with unicode content."""
        transformer = Transformer("test")
        
        with patch.object(transformer, 'finder', return_value="/path/to/unicode.txt"):
            result = transformer.open("unicode.txt")
        
        assert result == "content with unicode: 🚀 émojis"

    @patch("builtins.open", new_callable=mock_open)
    def test_open_very_large_file(self, mock_file):
        """Test open method with very large file content."""
        # Simulate large file content
        large_content = "line\n" * 100000
        mock_file.return_value.read.return_value = large_content
        
        transformer = Transformer("test")
        
        with patch.object(transformer, 'finder', return_value="/path/to/large.txt"):
            result = transformer.open("large.txt")
        
        assert len(result) == len(large_content)
        assert result.count('\n') == 100000