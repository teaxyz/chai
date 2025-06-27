#!/usr/bin/env pkgx uv run
"""
Comprehensive tests for package_managers/crates/main.py module.

Tests cover main workflow, deletion identification, fetching, parsing,
diffing, and database ingestion functionality.
"""

import uuid
from unittest.mock import MagicMock, Mock, patch

import pytest

from package_managers.crates.main import identify_deletions, main
from package_managers.crates.db import CratesDB
from package_managers.crates.transformer import CratesTransformer
from package_managers.crates.structs import Crate, CrateLatestVersion
from core.models import URL, LegacyDependency, Package, PackageURL
from core.structs import Cache, URLKey


class TestIdentifyDeletions:
    """Test cases for the identify_deletions function."""

    def test_identify_deletions_no_deletions(self, mock_config):
        """Test identify_deletions when no crates are deleted."""
        # Setup transformer with crates
        transformer = Mock(spec=CratesTransformer)
        transformer.crates = {
            1: Mock(id=1),
            2: Mock(id=2),
            3: Mock(id=3)
        }
        
        # Setup database with same crates
        db = Mock(spec=CratesDB)
        db.get_cargo_id_to_chai_id.return_value = {
            "1": uuid.uuid4(),
            "2": uuid.uuid4(),
            "3": uuid.uuid4()
        }
        
        deletions = identify_deletions(transformer, db)
        
        assert deletions == set()
        db.get_cargo_id_to_chai_id.assert_called_once()

    def test_identify_deletions_with_deletions(self, mock_config):
        """Test identify_deletions when some crates are deleted from registry."""
        # Setup transformer with fewer crates (some deleted)
        transformer = Mock(spec=CratesTransformer)
        transformer.crates = {
            1: Mock(id=1),
            3: Mock(id=3)
        }
        
        # Setup database with more crates (includes deleted ones)
        db = Mock(spec=CratesDB)
        db.get_cargo_id_to_chai_id.return_value = {
            "1": uuid.uuid4(),
            "2": uuid.uuid4(),  # This one is deleted from registry
            "3": uuid.uuid4(),
            "4": uuid.uuid4()   # This one is also deleted
        }
        
        deletions = identify_deletions(transformer, db)
        
        assert deletions == {2, 4}

    def test_identify_deletions_all_deleted(self, mock_config):
        """Test identify_deletions when all crates are deleted."""
        # Setup transformer with no crates
        transformer = Mock(spec=CratesTransformer)
        transformer.crates = {}
        
        # Setup database with crates
        db = Mock(spec=CratesDB)
        db.get_cargo_id_to_chai_id.return_value = {
            "1": uuid.uuid4(),
            "2": uuid.uuid4()
        }
        
        deletions = identify_deletions(transformer, db)
        
        assert deletions == {1, 2}

    def test_identify_deletions_empty_db(self, mock_config):
        """Test identify_deletions when database is empty."""
        transformer = Mock(spec=CratesTransformer)
        transformer.crates = {
            1: Mock(id=1),
            2: Mock(id=2)
        }
        
        db = Mock(spec=CratesDB)
        db.get_cargo_id_to_chai_id.return_value = {}
        
        deletions = identify_deletions(transformer, db)
        
        assert deletions == set()

    def test_identify_deletions_new_crates_only(self, mock_config):
        """Test identify_deletions with only new crates (none in db)."""
        transformer = Mock(spec=CratesTransformer)
        transformer.crates = {
            5: Mock(id=5),
            6: Mock(id=6)
        }
        
        db = Mock(spec=CratesDB)
        db.get_cargo_id_to_chai_id.return_value = {
            "1": uuid.uuid4(),
            "2": uuid.uuid4()
        }
        
        deletions = identify_deletions(transformer, db)
        
        # All DB crates are "deleted" since they're not in transformer
        assert deletions == {1, 2}


class TestMainFunction:
    """Test cases for the main function."""

    @patch("package_managers.crates.main.TarballFetcher")
    @patch("package_managers.crates.main.CratesTransformer")
    @patch("package_managers.crates.main.identify_deletions")
    @patch("package_managers.crates.main.Cache")
    @patch("package_managers.crates.main.Diff")
    def test_main_with_fetch_enabled(self, mock_diff_class, mock_cache_class, 
                                   mock_identify_deletions, mock_transformer_class, 
                                   mock_fetcher_class, mock_config):
        """Test main function with fetching enabled."""
        # Setup config
        mock_config.exec_config.fetch = True
        mock_config.exec_config.no_cache = False
        mock_config.exec_config.test = False
        mock_config.pm_config.source = "https://example.com/crates.tar.gz"
        
        # Setup mocks
        mock_fetcher = Mock()
        mock_fetcher.fetch.return_value = [Mock(), Mock()]  # Mock files
        mock_fetcher_class.return_value = mock_fetcher
        
        mock_transformer = Mock()
        mock_transformer.crates = {1: Mock(homepage="", repository="", documentation="")}
        mock_transformer_class.return_value = mock_transformer
        
        mock_identify_deletions.return_value = set()
        
        mock_db = Mock()
        mock_db.set_current_graph = Mock()
        mock_db.set_current_urls = Mock()
        mock_db.graph = Mock()
        mock_db.urls = Mock()
        mock_db.ingest = Mock()
        
        mock_cache = Mock()
        mock_cache_class.return_value = mock_cache
        
        mock_diff = Mock()
        mock_diff.diff_pkg.return_value = (uuid.uuid4(), None, None)
        mock_diff.diff_url.return_value = {}
        mock_diff.diff_pkg_url.return_value = ([], [])
        mock_diff.diff_deps.return_value = ([], [])
        mock_diff_class.return_value = mock_diff
        
        main(mock_config, mock_db)
        
        # Verify fetching was called
        mock_fetcher_class.assert_called_once_with(
            "crates", "https://example.com/crates.tar.gz", False, False
        )
        mock_fetcher.fetch.assert_called_once()
        mock_fetcher.write.assert_called_once()
        
        # Verify parsing and processing
        mock_transformer.parse.assert_called_once()
        mock_identify_deletions.assert_called_once()
        mock_db.set_current_graph.assert_called_once()
        mock_db.set_current_urls.assert_called_once()
        mock_db.ingest.assert_called_once()

    @patch("package_managers.crates.main.TarballFetcher")
    @patch("package_managers.crates.main.CratesTransformer")
    @patch("package_managers.crates.main.identify_deletions")
    @patch("package_managers.crates.main.Cache")
    @patch("package_managers.crates.main.Diff")
    def test_main_no_fetch_with_cache(self, mock_diff_class, mock_cache_class, 
                                    mock_identify_deletions, mock_transformer_class, 
                                    mock_fetcher_class, mock_config):
        """Test main function without fetching but with cache."""
        # Setup config
        mock_config.exec_config.fetch = False
        mock_config.exec_config.no_cache = False
        mock_config.exec_config.test = False
        
        # Setup mocks
        mock_fetcher = Mock()
        mock_fetcher_class.return_value = mock_fetcher
        
        mock_transformer = Mock()
        mock_transformer.crates = {}
        mock_transformer_class.return_value = mock_transformer
        
        mock_identify_deletions.return_value = set()
        
        mock_db = Mock()
        mock_db.set_current_graph = Mock()
        mock_db.set_current_urls = Mock()
        mock_db.graph = Mock()
        mock_db.urls = Mock()
        mock_db.ingest = Mock()
        
        main(mock_config, mock_db)
        
        # Verify fetching was not called but writing was
        mock_fetcher.fetch.assert_not_called()
        mock_fetcher.write.assert_called_once()

    @patch("package_managers.crates.main.TarballFetcher")
    @patch("package_managers.crates.main.CratesTransformer")
    @patch("package_managers.crates.main.identify_deletions")
    @patch("package_managers.crates.main.Cache")
    @patch("package_managers.crates.main.Diff")
    def test_main_with_deletions(self, mock_diff_class, mock_cache_class, 
                                mock_identify_deletions, mock_transformer_class, 
                                mock_fetcher_class, mock_config):
        """Test main function when deletions are identified."""
        # Setup config
        mock_config.exec_config.fetch = False
        mock_config.exec_config.no_cache = True
        
        # Setup mocks
        mock_fetcher = Mock()
        mock_fetcher_class.return_value = mock_fetcher
        
        mock_transformer = Mock()
        mock_transformer.crates = {}
        mock_transformer_class.return_value = mock_transformer
        
        # Simulate deletions found
        deletions = {123, 456}
        mock_identify_deletions.return_value = deletions
        
        mock_db = Mock()
        mock_db.delete_packages_by_import_id = Mock()
        mock_db.set_current_graph = Mock()
        mock_db.set_current_urls = Mock()
        mock_db.graph = Mock()
        mock_db.urls = Mock()
        mock_db.ingest = Mock()
        
        main(mock_config, mock_db)
        
        # Verify deletions were processed
        mock_identify_deletions.assert_called_once_with(mock_transformer, mock_db)
        mock_db.delete_packages_by_import_id.assert_called_once_with(deletions)

    @patch("package_managers.crates.main.TarballFetcher")
    @patch("package_managers.crates.main.CratesTransformer")
    @patch("package_managers.crates.main.identify_deletions")
    @patch("package_managers.crates.main.Cache")
    @patch("package_managers.crates.main.Diff")
    def test_main_diff_processing(self, mock_diff_class, mock_cache_class, 
                                mock_identify_deletions, mock_transformer_class, 
                                mock_fetcher_class, mock_config):
        """Test main function diff processing logic."""
        # Setup config
        mock_config.exec_config.fetch = False
        mock_config.exec_config.no_cache = True
        
        # Setup transformer with test crates
        mock_crate1 = Mock()
        mock_crate2 = Mock()
        mock_transformer = Mock()
        mock_transformer.crates = {1: mock_crate1, 2: mock_crate2}
        mock_transformer_class.return_value = mock_transformer
        
        mock_identify_deletions.return_value = set()
        
        # Setup database
        mock_db = Mock()
        mock_db.set_current_graph = Mock()
        mock_db.set_current_urls = Mock()
        mock_db.graph = Mock()
        mock_db.urls = Mock()
        mock_db.ingest = Mock()
        
        # Setup diff mock
        mock_diff = Mock()
        pkg_id1 = uuid.uuid4()
        pkg_id2 = uuid.uuid4()
        
        # Mock diff responses for each crate
        def diff_pkg_side_effect(crate):
            if crate == mock_crate1:
                return pkg_id1, Mock(spec=Package), {"id": pkg_id1, "updated": True}
            else:
                return pkg_id2, None, None
        
        mock_diff.diff_pkg.side_effect = diff_pkg_side_effect
        mock_diff.diff_url.return_value = {uuid.uuid4(): uuid.uuid4()}
        mock_diff.diff_pkg_url.return_value = ([Mock(spec=PackageURL)], [{"id": pkg_id1}])
        mock_diff.diff_deps.return_value = ([Mock(spec=LegacyDependency)], [Mock(spec=LegacyDependency)])
        
        mock_diff_class.return_value = mock_diff
        
        main(mock_config, mock_db)
        
        # Verify diff was called for each crate
        assert mock_diff.diff_pkg.call_count == 2
        assert mock_diff.diff_url.call_count == 2
        assert mock_diff.diff_pkg_url.call_count == 2
        assert mock_diff.diff_deps.call_count == 2
        
        # Verify ingest was called with aggregated data
        mock_db.ingest.assert_called_once()
        call_args = mock_db.ingest.call_args[0]
        new_packages, new_urls, new_package_urls, new_deps, removed_deps, updated_packages, updated_package_urls = call_args
        
        # Should have 1 new package (from crate1)
        assert len(new_packages) == 1
        # Should have 1 updated package (from crate1)
        assert len(updated_packages) == 1

    @patch("package_managers.crates.main.TarballFetcher")
    @patch("package_managers.crates.main.CratesTransformer")
    @patch("package_managers.crates.main.identify_deletions")
    @patch("package_managers.crates.main.Cache")
    @patch("package_managers.crates.main.Diff")
    def test_main_url_collection(self, mock_diff_class, mock_cache_class, 
                                mock_identify_deletions, mock_transformer_class, 
                                mock_fetcher_class, mock_config):
        """Test main function URL collection and caching."""
        # Setup config
        mock_config.exec_config.fetch = False
        mock_config.exec_config.no_cache = True
        
        # Setup transformer with crates having URLs
        mock_crate = Mock()
        mock_crate.homepage = "https://example.com"
        mock_crate.repository = "https://github.com/user/repo"
        mock_crate.documentation = "https://docs.example.com"
        
        mock_transformer = Mock()
        mock_transformer.crates = {1: mock_crate}
        mock_transformer_class.return_value = mock_transformer
        
        mock_identify_deletions.return_value = set()
        
        # Setup database
        mock_db = Mock()
        mock_db.set_current_graph = Mock()
        mock_db.set_current_urls = Mock()
        mock_db.graph = Mock()
        mock_db.urls = Mock()
        mock_db.ingest = Mock()
        
        # Setup diff mock
        mock_diff = Mock()
        mock_diff.diff_pkg.return_value = (uuid.uuid4(), None, None)
        mock_diff.diff_url.return_value = {}
        mock_diff.diff_pkg_url.return_value = ([], [])
        mock_diff.diff_deps.return_value = ([], [])
        mock_diff_class.return_value = mock_diff
        
        main(mock_config, mock_db)
        
        # Verify URLs were collected and passed to set_current_urls
        mock_db.set_current_urls.assert_called_once()
        url_set = mock_db.set_current_urls.call_args[0][0]
        assert "https://example.com" in url_set
        assert "https://github.com/user/repo" in url_set
        assert "https://docs.example.com" in url_set

    @patch("package_managers.crates.main.TarballFetcher")
    @patch("package_managers.crates.main.CratesTransformer")
    @patch("package_managers.crates.main.identify_deletions")
    @patch("package_managers.crates.main.Cache")
    @patch("package_managers.crates.main.Diff")
    def test_main_cache_construction(self, mock_diff_class, mock_cache_class, 
                                   mock_identify_deletions, mock_transformer_class, 
                                   mock_fetcher_class, mock_config):
        """Test main function cache construction."""
        # Setup config
        mock_config.exec_config.fetch = False
        mock_config.exec_config.no_cache = True
        
        # Setup mocks
        mock_transformer = Mock()
        mock_transformer.crates = {}
        mock_transformer_class.return_value = mock_transformer
        
        mock_identify_deletions.return_value = set()
        
        # Setup database with graph and URLs
        mock_graph = Mock()
        mock_graph.package_map = {"pkg1": Mock()}
        mock_graph.dependencies = {uuid.uuid4(): set()}
        
        mock_urls = Mock()
        mock_urls.url_map = {Mock(): Mock()}
        mock_urls.package_urls = {uuid.uuid4(): set()}
        
        mock_db = Mock()
        mock_db.set_current_graph = Mock()
        mock_db.set_current_urls = Mock()
        mock_db.graph = mock_graph
        mock_db.urls = mock_urls
        mock_db.ingest = Mock()
        
        # Setup cache mock
        mock_cache = Mock()
        mock_cache_class.return_value = mock_cache
        
        main(mock_config, mock_db)
        
        # Verify cache was constructed with correct parameters
        mock_cache_class.assert_called_once_with(
            mock_graph.package_map,
            mock_urls.url_map,
            mock_urls.package_urls,
            mock_graph.dependencies,
        )

    @patch("package_managers.crates.main.TarballFetcher")
    @patch("package_managers.crates.main.CratesTransformer")
    @patch("package_managers.crates.main.identify_deletions")
    def test_main_no_cache_scenario(self, mock_identify_deletions, mock_transformer_class, 
                                  mock_fetcher_class, mock_config):
        """Test main function with no_cache=True scenario."""
        # Setup config
        mock_config.exec_config.fetch = True
        mock_config.exec_config.no_cache = True
        mock_config.exec_config.test = True
        mock_config.pm_config.source = "test-source"
        
        # Setup mocks
        mock_fetcher = Mock()
        mock_fetcher_class.return_value = mock_fetcher
        
        mock_transformer = Mock()
        mock_transformer.crates = {}
        mock_transformer_class.return_value = mock_transformer
        
        mock_identify_deletions.return_value = set()
        
        mock_db = Mock()
        mock_db.set_current_graph = Mock()
        mock_db.set_current_urls = Mock()
        mock_db.graph = Mock()
        mock_db.urls = Mock()
        mock_db.ingest = Mock()
        
        main(mock_config, mock_db)
        
        # Verify fetcher was created with no_cache=True
        mock_fetcher_class.assert_called_once_with("crates", "test-source", True, True)
        
        # With no_cache=True, files should not be written
        mock_fetcher.write.assert_not_called()


# Integration tests
class TestMainIntegration:
    """Integration tests for main function workflow."""

    @patch("package_managers.crates.main.TarballFetcher")
    @patch("package_managers.crates.main.CratesTransformer")
    @patch("package_managers.crates.main.identify_deletions")
    @patch("package_managers.crates.main.Cache")
    @patch("package_managers.crates.main.Diff")
    def test_complete_workflow_integration(self, mock_diff_class, mock_cache_class, 
                                         mock_identify_deletions, mock_transformer_class, 
                                         mock_fetcher_class, mock_config):
        """Test complete main workflow integration."""
        # Setup realistic config
        mock_config.exec_config.fetch = True
        mock_config.exec_config.no_cache = False
        mock_config.exec_config.test = False
        mock_config.pm_config.source = "https://crates.io/data.tar.gz"
        
        # Setup realistic fetcher
        mock_files = [Mock(name="crates.csv"), Mock(name="versions.csv")]
        mock_fetcher = Mock()
        mock_fetcher.fetch.return_value = mock_files
        mock_fetcher_class.return_value = mock_fetcher
        
        # Setup realistic transformer
        mock_crate = Mock()
        mock_crate.homepage = "https://serde.rs"
        mock_crate.repository = "https://github.com/serde-rs/serde"
        mock_crate.documentation = "https://docs.serde.rs"
        
        mock_transformer = Mock()
        mock_transformer.crates = {1: mock_crate}
        mock_transformer_class.return_value = mock_transformer
        
        # Setup realistic deletions
        mock_identify_deletions.return_value = {999}  # One deleted crate
        
        # Setup realistic database
        mock_db = Mock()
        mock_db.delete_packages_by_import_id = Mock()
        mock_db.set_current_graph = Mock()
        mock_db.set_current_urls = Mock()
        mock_db.graph = Mock()
        mock_db.urls = Mock()
        mock_db.ingest = Mock()
        
        # Setup realistic diff
        pkg_id = uuid.uuid4()
        mock_package = Mock(spec=Package)
        mock_url_key = URLKey("https://serde.rs", uuid.uuid4())
        mock_url = Mock(spec=URL)
        mock_package_url = Mock(spec=PackageURL)
        mock_dependency = Mock(spec=LegacyDependency)
        
        mock_diff = Mock()
        mock_diff.diff_pkg.return_value = (pkg_id, mock_package, None)
        mock_diff.diff_url.return_value = {mock_url_key: mock_url}
        mock_diff.diff_pkg_url.return_value = ([mock_package_url], [])
        mock_diff.diff_deps.return_value = ([mock_dependency], [])
        mock_diff_class.return_value = mock_diff
        
        # Execute main
        main(mock_config, mock_db)
        
        # Verify complete workflow
        mock_fetcher.fetch.assert_called_once()
        mock_fetcher.write.assert_called_once_with(mock_files)
        mock_transformer.parse.assert_called_once()
        mock_identify_deletions.assert_called_once()
        mock_db.delete_packages_by_import_id.assert_called_once_with({999})
        
        # Verify data processing
        mock_db.set_current_graph.assert_called_once()
        mock_db.set_current_urls.assert_called_once()
        mock_db.ingest.assert_called_once()
        
        # Verify ingest parameters
        ingest_args = mock_db.ingest.call_args[0]
        new_packages, new_urls, new_package_urls, new_deps, removed_deps, updated_packages, updated_package_urls = ingest_args
        
        assert len(new_packages) == 1
        assert len(new_urls) == 1
        assert len(new_package_urls) == 1
        assert len(new_deps) == 1

    def test_main_error_handling(self, mock_config):
        """Test main function error handling."""
        mock_config.exec_config.fetch = False
        mock_config.exec_config.no_cache = True
        
        mock_db = Mock()
        mock_db.set_current_graph.side_effect = Exception("Database connection failed")
        
        # Should propagate the exception
        with pytest.raises(Exception, match="Database connection failed"):
            main(mock_config, mock_db)


# Edge case tests
class TestMainEdgeCases:
    """Test edge cases and boundary conditions."""

    @patch("package_managers.crates.main.TarballFetcher")
    @patch("package_managers.crates.main.CratesTransformer")
    @patch("package_managers.crates.main.identify_deletions")
    @patch("package_managers.crates.main.Cache")
    @patch("package_managers.crates.main.Diff")
    def test_main_empty_crates(self, mock_diff_class, mock_cache_class, 
                             mock_identify_deletions, mock_transformer_class, 
                             mock_fetcher_class, mock_config):
        """Test main function with no crates to process."""
        # Setup config
        mock_config.exec_config.fetch = False
        mock_config.exec_config.no_cache = True
        
        # Setup empty transformer
        mock_transformer = Mock()
        mock_transformer.crates = {}  # No crates
        mock_transformer_class.return_value = mock_transformer
        
        mock_identify_deletions.return_value = set()
        
        # Setup database
        mock_db = Mock()
        mock_db.set_current_graph = Mock()
        mock_db.set_current_urls = Mock()
        mock_db.graph = Mock()
        mock_db.urls = Mock()
        mock_db.ingest = Mock()
        
        main(mock_config, mock_db)
        
        # Should still call ingest with empty data
        mock_db.ingest.assert_called_once()
        ingest_args = mock_db.ingest.call_args[0]
        new_packages, new_urls, new_package_urls, new_deps, removed_deps, updated_packages, updated_package_urls = ingest_args
        
        assert len(new_packages) == 0
        assert len(new_urls) == 0
        assert len(new_package_urls) == 0
        assert len(new_deps) == 0
        assert len(removed_deps) == 0
        assert len(updated_packages) == 0
        assert len(updated_package_urls) == 0

    def test_identify_deletions_type_conversion(self):
        """Test identify_deletions handles type conversion correctly."""
        transformer = Mock()
        transformer.crates = {1: Mock(id=1), 2: Mock(id=2)}  # int keys
        
        db = Mock()
        db.get_cargo_id_to_chai_id.return_value = {
            "1": uuid.uuid4(),  # string keys in DB
            "3": uuid.uuid4()
        }
        
        deletions = identify_deletions(transformer, db)
        
        # Should convert string "3" to int 3 for comparison
        assert deletions == {3}

    def test_identify_deletions_large_dataset(self):
        """Test identify_deletions with large dataset."""
        # Setup large transformer dataset
        transformer = Mock()
        transformer.crates = {i: Mock(id=i) for i in range(1, 1001)}  # 1000 crates
        
        # Setup large DB dataset with some deletions
        db = Mock()
        db_mapping = {str(i): uuid.uuid4() for i in range(1, 1101)}  # 1100 crates in DB
        db.get_cargo_id_to_chai_id.return_value = db_mapping
        
        deletions = identify_deletions(transformer, db)
        
        # Should identify 100 deletions (1001-1100)
        expected_deletions = set(range(1001, 1101))
        assert deletions == expected_deletions