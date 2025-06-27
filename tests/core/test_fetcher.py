#!/usr/bin/env pkgx uv run
"""
Comprehensive tests for core/fetcher.py module.

Tests cover Fetcher base class, TarballFetcher, GZipFetcher, and GitFetcher
classes with various data types and edge cases.
"""

import gzip
import json
import os
import tarfile
from io import BytesIO
from unittest.mock import MagicMock, Mock, patch, mock_open

import pytest
import requests

from core.fetcher import Data, Fetcher, TarballFetcher, GZipFetcher, GitFetcher


class TestData:
    """Test cases for the Data dataclass."""

    def test_data_creation(self):
        """Test Data dataclass creation with various content types."""
        # Test with JSON content
        json_data = Data("path/to", "file.json", {"key": "value"})
        assert json_data.file_path == "path/to"
        assert json_data.file_name == "file.json"
        assert json_data.content == {"key": "value"}

        # Test with bytes content
        bytes_data = Data("path", "file.bin", b"binary content")
        assert bytes_data.content == b"binary content"

        # Test with string content
        str_data = Data("", "file.txt", "text content")
        assert str_data.content == "text content"


class TestFetcher:
    """Test cases for the Fetcher base class."""

    def test_fetcher_init(self):
        """Test Fetcher initialization."""
        fetcher = Fetcher("test-name", "https://example.com", no_cache=True, test=True)
        
        assert fetcher.name == "test-name"
        assert fetcher.source == "https://example.com"
        assert fetcher.output == "data/test-name"
        assert fetcher.no_cache is True
        assert fetcher.test is True

    @patch("os.makedirs")
    @patch("builtins.open", new_callable=mock_open)
    @patch("core.fetcher.datetime")
    def test_write_json_content(self, mock_datetime, mock_file, mock_makedirs):
        """Test write method with JSON content."""
        mock_datetime.now.return_value.strftime.return_value = "2023-12-01"
        
        fetcher = Fetcher("test", "source", False, False)
        
        json_data = Data("subdir", "test.json", {"key": "value"})
        
        with patch.object(fetcher, 'update_symlink') as mock_symlink:
            fetcher.write([json_data])
        
        # Verify directory creation
        mock_makedirs.assert_called_with("data/test/2023-12-01/subdir", exist_ok=True)
        
        # Verify file writing
        mock_file.assert_called_with("data/test/2023-12-01/subdir/test.json", "wb")
        written_content = mock_file().write.call_args[0][0]
        assert written_content == b'{"key": "value"}'
        
        # Verify symlink update
        mock_symlink.assert_called_once_with("2023-12-01")

    @patch("os.makedirs")
    @patch("builtins.open", new_callable=mock_open)
    @patch("core.fetcher.datetime")
    def test_write_string_content(self, mock_datetime, mock_file, mock_makedirs):
        """Test write method with string content."""
        mock_datetime.now.return_value.strftime.return_value = "2023-12-01"
        
        fetcher = Fetcher("test", "source", False, False)
        
        str_data = Data("", "test.txt", "text content")
        
        with patch.object(fetcher, 'update_symlink'):
            fetcher.write([str_data])
        
        # Verify file writing with string converted to bytes
        written_content = mock_file().write.call_args[0][0]
        assert written_content == b'text content'

    @patch("os.makedirs")
    @patch("builtins.open", new_callable=mock_open)
    @patch("core.fetcher.datetime")
    def test_write_bytes_content(self, mock_datetime, mock_file, mock_makedirs):
        """Test write method with bytes content."""
        mock_datetime.now.return_value.strftime.return_value = "2023-12-01"
        
        fetcher = Fetcher("test", "source", False, False)
        
        bytes_data = Data("", "test.bin", b"binary content")
        
        with patch.object(fetcher, 'update_symlink'):
            fetcher.write([bytes_data])
        
        # Verify file writing with bytes content
        written_content = mock_file().write.call_args[0][0]
        assert written_content == b'binary content'

    @patch("os.makedirs")
    @patch("builtins.open", new_callable=mock_open)
    @patch("core.fetcher.datetime")
    def test_write_list_content(self, mock_datetime, mock_file, mock_makedirs):
        """Test write method with list content (converted to JSON)."""
        mock_datetime.now.return_value.strftime.return_value = "2023-12-01"
        
        fetcher = Fetcher("test", "source", False, False)
        
        list_data = Data("", "test.json", ["item1", "item2"])
        
        with patch.object(fetcher, 'update_symlink'):
            fetcher.write([list_data])
        
        # Verify JSON serialization
        written_content = mock_file().write.call_args[0][0]
        assert written_content == b'["item1", "item2"]'

    @patch("os.makedirs")
    @patch("builtins.open", new_callable=mock_open)
    @patch("core.fetcher.datetime")
    def test_write_multiple_files(self, mock_datetime, mock_file, mock_makedirs):
        """Test write method with multiple files."""
        mock_datetime.now.return_value.strftime.return_value = "2023-12-01"
        
        fetcher = Fetcher("test", "source", False, False)
        
        files = [
            Data("dir1", "file1.txt", "content1"),
            Data("dir2", "file2.json", {"key": "value2"})
        ]
        
        with patch.object(fetcher, 'update_symlink'):
            fetcher.write(files)
        
        # Verify both directories were created
        expected_calls = [
            (("data/test/2023-12-01/dir1",), {"exist_ok": True}),
            (("data/test/2023-12-01/dir2",), {"exist_ok": True})
        ]
        assert mock_makedirs.call_count == 2

    @patch("os.path.islink")
    @patch("os.remove")
    @patch("os.symlink")
    def test_update_symlink_existing_link(self, mock_symlink, mock_remove, mock_islink):
        """Test update_symlink when symlink already exists."""
        mock_islink.return_value = True
        
        fetcher = Fetcher("test", "source", False, False)
        fetcher.update_symlink("2023-12-01")
        
        # Should remove existing symlink first
        mock_remove.assert_called_once_with("data/test/latest")
        mock_symlink.assert_called_once_with("2023-12-01", "data/test/latest")

    @patch("os.path.islink")
    @patch("os.remove")
    @patch("os.symlink")
    def test_update_symlink_no_existing_link(self, mock_symlink, mock_remove, mock_islink):
        """Test update_symlink when no symlink exists."""
        mock_islink.return_value = False
        
        fetcher = Fetcher("test", "source", False, False)
        fetcher.update_symlink("2023-12-01")
        
        # Should not try to remove non-existent symlink
        mock_remove.assert_not_called()
        mock_symlink.assert_called_once_with("2023-12-01", "data/test/latest")

    @patch("requests.get")
    def test_fetch_success(self, mock_get):
        """Test successful fetch operation."""
        mock_response = Mock()
        mock_response.content = b"fetched content"
        mock_get.return_value = mock_response
        
        fetcher = Fetcher("test", "https://example.com/data", False, False)
        result = fetcher.fetch()
        
        mock_get.assert_called_once_with("https://example.com/data")
        mock_response.raise_for_status.assert_called_once()
        assert result == b"fetched content"

    @patch("requests.get")
    def test_fetch_http_error(self, mock_get):
        """Test fetch with HTTP error."""
        mock_response = Mock()
        mock_response.raise_for_status.side_effect = requests.HTTPError("404 Not Found")
        mock_get.return_value = mock_response
        
        fetcher = Fetcher("test", "https://example.com/notfound", False, False)
        
        with pytest.raises(requests.HTTPError):
            fetcher.fetch()

    def test_fetch_no_source(self):
        """Test fetch with no source URL."""
        fetcher = Fetcher("test", "", False, False)
        
        with pytest.raises(ValueError, match="source is not set"):
            fetcher.fetch()

    def test_fetch_empty_source(self):
        """Test fetch with empty source URL after initialization."""
        fetcher = Fetcher("test", "https://example.com", False, False)
        # Simulate source being cleared after initialization
        fetcher.source = ""
        
        with pytest.raises(ValueError, match="source is not set"):
            fetcher.fetch()

    @patch("shutil.rmtree")
    @patch("os.makedirs")
    def test_cleanup_with_no_cache(self, mock_makedirs, mock_rmtree):
        """Test cleanup when no_cache is True."""
        fetcher = Fetcher("test", "source", no_cache=True, test=False)
        fetcher.cleanup()
        
        mock_rmtree.assert_called_once_with("data/test", ignore_errors=True)
        mock_makedirs.assert_called_once_with("data/test", exist_ok=True)

    @patch("shutil.rmtree")
    @patch("os.makedirs")
    def test_cleanup_without_no_cache(self, mock_makedirs, mock_rmtree):
        """Test cleanup when no_cache is False."""
        fetcher = Fetcher("test", "source", no_cache=False, test=False)
        fetcher.cleanup()
        
        # Should not clean up when no_cache is False
        mock_rmtree.assert_not_called()
        mock_makedirs.assert_not_called()


class TestTarballFetcher:
    """Test cases for the TarballFetcher class."""

    def test_tarball_fetcher_init(self):
        """Test TarballFetcher initialization."""
        fetcher = TarballFetcher("test", "https://example.com/archive.tar.gz", False, False)
        
        assert fetcher.name == "test"
        assert fetcher.source == "https://example.com/archive.tar.gz"
        assert isinstance(fetcher, Fetcher)

    @patch("requests.get")
    def test_fetch_tarball(self, mock_get):
        """Test fetching and extracting tarball."""
        # Create a mock tarball content
        tar_buffer = BytesIO()
        with tarfile.open(fileobj=tar_buffer, mode="w:gz") as tar:
            # Add a test file to the tarball
            file_content = b"test file content"
            file_info = tarfile.TarInfo(name="test/file.txt")
            file_info.size = len(file_content)
            tar.addfile(file_info, BytesIO(file_content))
        
        tar_content = tar_buffer.getvalue()
        
        mock_response = Mock()
        mock_response.content = tar_content
        mock_get.return_value = mock_response
        
        fetcher = TarballFetcher("test", "https://example.com/archive.tar.gz", False, False)
        result = fetcher.fetch()
        
        assert len(result) == 1
        assert isinstance(result[0], Data)
        assert result[0].file_path == "test"
        assert result[0].file_name == "file.txt"
        assert result[0].content == b"test file content"

    @patch("requests.get")
    def test_fetch_tarball_multiple_files(self, mock_get):
        """Test fetching tarball with multiple files."""
        tar_buffer = BytesIO()
        with tarfile.open(fileobj=tar_buffer, mode="w:gz") as tar:
            # Add multiple test files
            files = [
                ("dir1/file1.txt", b"content1"),
                ("dir2/subdir/file2.json", b'{"key": "value"}'),
                ("root.md", b"# Root file")
            ]
            
            for file_path, content in files:
                file_info = tarfile.TarInfo(name=file_path)
                file_info.size = len(content)
                tar.addfile(file_info, BytesIO(content))
        
        tar_content = tar_buffer.getvalue()
        
        mock_response = Mock()
        mock_response.content = tar_content
        mock_get.return_value = mock_response
        
        fetcher = TarballFetcher("test", "https://example.com/archive.tar.gz", False, False)
        result = fetcher.fetch()
        
        assert len(result) == 3
        
        # Check file1.txt
        file1 = next(f for f in result if f.file_name == "file1.txt")
        assert file1.file_path == "dir1"
        assert file1.content == b"content1"
        
        # Check file2.json
        file2 = next(f for f in result if f.file_name == "file2.json")
        assert file2.file_path == "dir2/subdir"
        assert file2.content == b'{"key": "value"}'
        
        # Check root.md
        root_file = next(f for f in result if f.file_name == "root.md")
        assert root_file.file_path == ""
        assert root_file.content == b"# Root file"

    @patch("requests.get")
    def test_fetch_tarball_skip_directories(self, mock_get):
        """Test that directories are skipped in tarball extraction."""
        tar_buffer = BytesIO()
        with tarfile.open(fileobj=tar_buffer, mode="w:gz") as tar:
            # Add a directory (should be skipped)
            dir_info = tarfile.TarInfo(name="test_dir/")
            dir_info.type = tarfile.DIRTYPE
            tar.addfile(dir_info)
            
            # Add a file (should be extracted)
            file_content = b"file content"
            file_info = tarfile.TarInfo(name="test_dir/file.txt")
            file_info.size = len(file_content)
            tar.addfile(file_info, BytesIO(file_content))
        
        tar_content = tar_buffer.getvalue()
        
        mock_response = Mock()
        mock_response.content = tar_content
        mock_get.return_value = mock_response
        
        fetcher = TarballFetcher("test", "https://example.com/archive.tar.gz", False, False)
        result = fetcher.fetch()
        
        # Should only extract the file, not the directory
        assert len(result) == 1
        assert result[0].file_name == "file.txt"


class TestGZipFetcher:
    """Test cases for the GZipFetcher class."""

    def test_gzip_fetcher_init(self):
        """Test GZipFetcher initialization."""
        fetcher = GZipFetcher("test", "https://example.com/file.gz", False, False, "path", "file.txt")
        
        assert fetcher.name == "test"
        assert fetcher.source == "https://example.com/file.gz"
        assert fetcher.file_path == "path"
        assert fetcher.file_name == "file.txt"
        assert isinstance(fetcher, Fetcher)

    @patch("requests.get")
    def test_fetch_gzip(self, mock_get):
        """Test fetching and decompressing gzip file."""
        original_content = "This is test content for compression"
        compressed_content = gzip.compress(original_content.encode('utf-8'))
        
        mock_response = Mock()
        mock_response.content = compressed_content
        mock_get.return_value = mock_response
        
        fetcher = GZipFetcher("test", "https://example.com/file.gz", False, False, "data", "file.txt")
        result = fetcher.fetch()
        
        assert len(result) == 1
        assert isinstance(result[0], Data)
        assert result[0].file_path == "data"
        assert result[0].file_name == "file.txt"
        assert result[0].content == original_content.encode('utf-8')

    @patch("requests.get")
    def test_fetch_gzip_large_content(self, mock_get):
        """Test fetching large gzip file."""
        # Create large content to test compression
        original_content = "Large content line\n" * 1000
        compressed_content = gzip.compress(original_content.encode('utf-8'))
        
        mock_response = Mock()
        mock_response.content = compressed_content
        mock_get.return_value = mock_response
        
        fetcher = GZipFetcher("test", "https://example.com/large.gz", False, False, "", "large.txt")
        result = fetcher.fetch()
        
        assert len(result) == 1
        assert result[0].content == original_content.encode('utf-8')

    @patch("requests.get")
    def test_fetch_gzip_invalid_format(self, mock_get):
        """Test fetching invalid gzip content."""
        mock_response = Mock()
        mock_response.content = b"not gzip content"
        mock_get.return_value = mock_response
        
        fetcher = GZipFetcher("test", "https://example.com/invalid.gz", False, False, "", "file.txt")
        
        with pytest.raises(gzip.BadGzipFile):
            fetcher.fetch()


class TestGitFetcher:
    """Test cases for the GitFetcher class."""

    def test_git_fetcher_init(self):
        """Test GitFetcher initialization."""
        fetcher = GitFetcher("test", "https://github.com/user/repo.git", False, False)
        
        assert fetcher.name == "test"
        assert fetcher.source == "https://github.com/user/repo.git"
        assert isinstance(fetcher, Fetcher)

    @patch("git.Repo.clone_from")
    @patch("os.makedirs")
    @patch("core.fetcher.datetime")
    def test_fetch_git_repository(self, mock_datetime, mock_makedirs, mock_clone):
        """Test fetching git repository."""
        mock_datetime.now.return_value.strftime.return_value = "2023-12-01"
        mock_repo = Mock()
        mock_clone.return_value = mock_repo
        
        fetcher = GitFetcher("test", "https://github.com/user/repo.git", False, False)
        
        with patch.object(fetcher, 'update_symlink') as mock_symlink:
            result = fetcher.fetch()
        
        # Verify directory creation
        mock_makedirs.assert_called_once_with("data/test/2023-12-01", exist_ok=True)
        
        # Verify git clone
        mock_clone.assert_called_once_with(
            "https://github.com/user/repo.git",
            "data/test/2023-12-01",
            depth=1,
            branch="main"
        )
        
        # Verify symlink update
        mock_symlink.assert_called_once_with("2023-12-01")
        
        # Verify return value
        assert result == "data/test/2023-12-01"

    @patch("git.Repo.clone_from")
    @patch("os.makedirs")
    @patch("core.fetcher.datetime")
    def test_fetch_git_clone_failure(self, mock_datetime, mock_makedirs, mock_clone):
        """Test git fetch with clone failure."""
        mock_datetime.now.return_value.strftime.return_value = "2023-12-01"
        mock_clone.side_effect = Exception("Git clone failed")
        
        fetcher = GitFetcher("test", "https://github.com/invalid/repo.git", False, False)
        
        with pytest.raises(Exception, match="Git clone failed"):
            fetcher.fetch()

    @patch("git.Repo.clone_from")
    @patch("os.makedirs")
    @patch("core.fetcher.datetime")
    def test_fetch_git_with_custom_branch(self, mock_datetime, mock_makedirs, mock_clone):
        """Test that GitFetcher always uses main branch."""
        mock_datetime.now.return_value.strftime.return_value = "2023-12-01"
        mock_repo = Mock()
        mock_clone.return_value = mock_repo
        
        fetcher = GitFetcher("test", "https://github.com/user/repo.git", False, False)
        
        with patch.object(fetcher, 'update_symlink'):
            fetcher.fetch()
        
        # Verify that main branch is always used
        mock_clone.assert_called_once_with(
            "https://github.com/user/repo.git",
            "data/test/2023-12-01",
            depth=1,
            branch="main"
        )


# Integration tests
class TestFetcherIntegration:
    """Integration tests for fetcher classes."""

    @patch("os.makedirs")
    @patch("builtins.open", new_callable=mock_open)
    @patch("core.fetcher.datetime")
    @patch("requests.get")
    def test_complete_workflow(self, mock_get, mock_datetime, mock_file, mock_makedirs):
        """Test complete fetcher workflow from fetch to write."""
        mock_datetime.now.return_value.strftime.return_value = "2023-12-01"
        
        # Mock HTTP response
        mock_response = Mock()
        mock_response.content = b'{"data": "test"}'
        mock_get.return_value = mock_response
        
        fetcher = Fetcher("integration-test", "https://api.example.com/data", False, False)
        
        # Fetch data
        content = fetcher.fetch()
        
        # Create Data object and write
        data_obj = Data("api", "data.json", json.loads(content.decode()))
        
        with patch.object(fetcher, 'update_symlink'):
            fetcher.write([data_obj])
        
        # Verify the complete workflow
        mock_get.assert_called_once()
        mock_makedirs.assert_called_once()
        assert mock_file().write.called