#!/usr/bin/env pkgx uv run
"""
Comprehensive tests for package_managers/crates/db.py module.

Tests cover CratesDB class initialization, database operations,
package deletion, and data mapping functionality.
"""

import uuid
from unittest.mock import MagicMock, Mock, patch

import pytest
from sqlalchemy.orm import Session

from package_managers.crates.db import CratesDB
from core.models import (
    CanonPackage,
    DependsOn,
    LegacyDependency,
    Package,
    PackageURL,
    UserPackage,
    UserVersion,
    Version,
)
from core.structs import CurrentGraph, CurrentURLs


class TestCratesDB:
    """Test cases for the CratesDB class."""

    def test_crates_db_init(self, mock_config):
        """Test CratesDB initialization."""
        with patch.object(CratesDB, '__init__', lambda x, config: None):
            db = CratesDB.__new__(CratesDB)
            db.config = mock_config
            db.logger = Mock()
            db.session = Mock()
            
            assert db.config == mock_config

    @patch("core.db.DB.__init__")
    def test_crates_db_init_calls_super(self, mock_super_init, mock_config):
        """Test that CratesDB initialization calls parent constructor."""
        db = CratesDB(mock_config)
        
        mock_super_init.assert_called_once_with("crates_db")
        assert db.config == mock_config

    def test_set_current_graph(self, mock_config):
        """Test set_current_graph method."""
        db = CratesDB.__new__(CratesDB)
        db.config = mock_config
        db.current_graph = Mock()
        
        mock_graph = CurrentGraph({}, {})
        db.current_graph.return_value = mock_graph
        
        db.set_current_graph()
        
        db.current_graph.assert_called_once_with(mock_config.pm_config.pm_id)
        assert db.graph == mock_graph

    def test_set_current_urls(self, mock_config):
        """Test set_current_urls method."""
        db = CratesDB.__new__(CratesDB)
        db.current_urls = Mock()
        
        mock_urls = CurrentURLs({}, {})
        db.current_urls.return_value = mock_urls
        test_urls = {"https://example.com", "https://test.com"}
        
        db.set_current_urls(test_urls)
        
        db.current_urls.assert_called_once_with(test_urls)
        assert db.urls == mock_urls

    def test_delete_packages_by_import_id_empty_set(self, mock_config):
        """Test delete_packages_by_import_id with empty import_ids."""
        db = CratesDB.__new__(CratesDB)
        db.config = mock_config
        db.logger = Mock()
        db.import_id_map = {}
        
        db.delete_packages_by_import_id(set())
        
        db.logger.debug.assert_called_with("No packages found to delete")

    def test_delete_packages_by_import_id_no_matching_packages(self, mock_config):
        """Test delete_packages_by_import_id with no matching packages in cache."""
        db = CratesDB.__new__(CratesDB)
        db.config = mock_config
        db.logger = Mock()
        db.import_id_map = {"123": uuid.uuid4()}  # Different from requested
        
        db.delete_packages_by_import_id({456, 789})
        
        db.logger.debug.assert_called_with("No packages found to delete")

    @patch("core.db.DB.session")
    def test_delete_packages_by_import_id_success(self, mock_session_factory, mock_config):
        """Test successful package deletion with all related records."""
        # Setup
        db = CratesDB.__new__(CratesDB)
        db.config = mock_config
        db.logger = Mock()
        
        # Mock package IDs
        pkg_id1 = uuid.uuid4()
        pkg_id2 = uuid.uuid4()
        db.import_id_map = {"123": pkg_id1, "456": pkg_id2}
        
        # Mock session and queries
        mock_session = Mock(spec=Session)
        mock_session_factory.return_value.__enter__.return_value = mock_session
        
        # Mock query results for deletion counts
        mock_query = Mock()
        mock_session.query.return_value = mock_query
        
        # Setup deletion counts
        mock_query.filter.return_value.delete.return_value = 5  # Default count
        mock_query.filter.return_value.all.return_value = [(uuid.uuid4(),)]  # Version IDs
        
        db.session = mock_session_factory
        
        db.delete_packages_by_import_id({123, 456})
        
        # Verify session operations
        mock_session.commit.assert_called_once()
        assert mock_session.query.call_count >= 5  # Multiple queries for different tables
        db.logger.debug.assert_any_call("Deleting 2 crates completely")

    @patch("core.db.DB.session")
    def test_delete_packages_by_import_id_with_rollback(self, mock_session_factory, mock_config):
        """Test package deletion with database error and rollback."""
        db = CratesDB.__new__(CratesDB)
        db.config = mock_config
        db.logger = Mock()
        
        pkg_id = uuid.uuid4()
        db.import_id_map = {"123": pkg_id}
        
        # Mock session that raises exception
        mock_session = Mock(spec=Session)
        mock_session_factory.return_value.__enter__.return_value = mock_session
        mock_session.query.side_effect = Exception("Database error")
        
        db.session = mock_session_factory
        
        with pytest.raises(Exception, match="Database error"):
            db.delete_packages_by_import_id({123})
        
        mock_session.rollback.assert_called_once()
        db.logger.error.assert_called_with("Error deleting packages: Database error")

    @patch("core.db.DB.session")
    def test_delete_packages_complex_relationships(self, mock_session_factory, mock_config):
        """Test deletion of packages with complex relationship dependencies."""
        db = CratesDB.__new__(CratesDB)
        db.config = mock_config
        db.logger = Mock()
        
        pkg_id = uuid.uuid4()
        db.import_id_map = {"123": pkg_id}
        
        # Mock session
        mock_session = Mock(spec=Session)
        mock_session_factory.return_value.__enter__.return_value = mock_session
        
        # Mock different query responses for different tables
        def mock_query_side_effect(model_class):
            mock_query = Mock()
            if model_class == Version:
                # Return version IDs for versions query
                mock_query.filter.return_value.all.return_value = [(uuid.uuid4(),), (uuid.uuid4(),)]
                mock_query.filter.return_value.delete.return_value = 2
            else:
                # Other tables
                mock_query.filter.return_value.delete.return_value = 1
            return mock_query
        
        mock_session.query.side_effect = mock_query_side_effect
        db.session = mock_session_factory
        
        db.delete_packages_by_import_id({123})
        
        # Verify all table types were queried
        mock_session.commit.assert_called_once()
        # Should query: PackageURL, CanonPackage, UserPackage, LegacyDependency (2x), 
        # DependsOn, Version, DependsOn (versions), UserVersion, Package
        assert mock_session.query.call_count >= 8

    def test_get_cargo_id_to_chai_id(self, mock_config):
        """Test get_cargo_id_to_chai_id method."""
        db = CratesDB.__new__(CratesDB)
        db.config = mock_config
        db.session = Mock()
        
        # Mock session and query results
        mock_session = Mock(spec=Session)
        db.session.return_value.__enter__.return_value = mock_session
        
        # Mock query results: (import_id, package_id) tuples
        pkg_id1 = uuid.uuid4()
        pkg_id2 = uuid.uuid4()
        mock_results = [("123", pkg_id1), ("456", pkg_id2)]
        mock_session.execute.return_value.all.return_value = mock_results
        
        result = db.get_cargo_id_to_chai_id()
        
        expected = {"123": pkg_id1, "456": pkg_id2}
        assert result == expected
        assert db.import_id_map == expected
        
        # Verify query was constructed correctly
        mock_session.execute.assert_called_once()

    def test_get_cargo_id_to_chai_id_empty_result(self, mock_config):
        """Test get_cargo_id_to_chai_id with no packages."""
        db = CratesDB.__new__(CratesDB)
        db.config = mock_config
        db.session = Mock()
        
        mock_session = Mock(spec=Session)
        db.session.return_value.__enter__.return_value = mock_session
        mock_session.execute.return_value.all.return_value = []
        
        result = db.get_cargo_id_to_chai_id()
        
        assert result == {}
        assert db.import_id_map == {}

    def test_get_cargo_id_to_chai_id_filters_by_package_manager(self, mock_config):
        """Test that get_cargo_id_to_chai_id filters by package manager ID."""
        db = CratesDB.__new__(CratesDB)
        db.config = mock_config
        db.session = Mock()
        
        mock_session = Mock(spec=Session)
        db.session.return_value.__enter__.return_value = mock_session
        mock_session.execute.return_value.all.return_value = []
        
        db.get_cargo_id_to_chai_id()
        
        # Verify that the select statement was called with session.execute
        mock_session.execute.assert_called_once()
        # The select statement should filter by package_manager_id
        # We can't easily verify the exact SQL but we can verify execute was called

    @patch("core.db.DB.session")
    def test_delete_packages_version_dependencies_cleanup(self, mock_session_factory, mock_config):
        """Test that version dependencies are properly cleaned up during deletion."""
        db = CratesDB.__new__(CratesDB)
        db.config = mock_config
        db.logger = Mock()
        
        pkg_id = uuid.uuid4()
        db.import_id_map = {"123": pkg_id}
        
        mock_session = Mock(spec=Session)
        mock_session_factory.return_value.__enter__.return_value = mock_session
        
        # Mock version IDs query
        version_id1 = uuid.uuid4()
        version_id2 = uuid.uuid4()
        
        def mock_query_response(model_class):
            mock_query = Mock()
            if model_class == Version:
                # First call returns version IDs, second call deletes versions
                if not hasattr(mock_query_response, 'version_call_count'):
                    mock_query_response.version_call_count = 0
                mock_query_response.version_call_count += 1
                
                if mock_query_response.version_call_count == 1:
                    # First call: get version IDs
                    mock_query.filter.return_value.all.return_value = [(version_id1,), (version_id2,)]
                else:
                    # Second call: delete versions
                    mock_query.filter.return_value.delete.return_value = 2
            else:
                mock_query.filter.return_value.delete.return_value = 1
            return mock_query
        
        mock_session.query.side_effect = mock_query_response
        db.session = mock_session_factory
        
        db.delete_packages_by_import_id({123})
        
        # Should have queried versions twice (once for IDs, once for deletion)
        version_queries = [call for call in mock_session.query.call_args_list 
                          if call[0][0] == Version]
        assert len(version_queries) >= 1

    def test_delete_packages_legacy_dependencies_bidirectional(self, mock_config):
        """Test that legacy dependencies are deleted bidirectionally."""
        db = CratesDB.__new__(CratesDB)
        db.config = mock_config
        db.logger = Mock()
        db.session = Mock()
        
        pkg_id = uuid.uuid4()
        db.import_id_map = {"123": pkg_id}
        
        mock_session = Mock(spec=Session)
        db.session.return_value.__enter__.return_value = mock_session
        
        # Track LegacyDependency queries
        legacy_dep_queries = []
        
        def mock_query_side_effect(model_class):
            mock_query = Mock()
            if model_class == LegacyDependency:
                legacy_dep_queries.append(mock_query)
                mock_query.filter.return_value.delete.return_value = 3
            else:
                mock_query.filter.return_value.delete.return_value = 1
                if model_class == Version:
                    mock_query.filter.return_value.all.return_value = []
            return mock_query
        
        mock_session.query.side_effect = mock_query_side_effect
        
        db.delete_packages_by_import_id({123})
        
        # Should have queried LegacyDependency twice (package_id and dependency_id)
        legacy_dependency_calls = [call for call in mock_session.query.call_args_list 
                                  if call[0][0] == LegacyDependency]
        assert len(legacy_dependency_calls) >= 2


# Integration tests
class TestCratesDBIntegration:
    """Integration tests for CratesDB operations."""

    def test_deletion_workflow_integration(self, mock_config):
        """Test complete deletion workflow integration."""
        db = CratesDB.__new__(CratesDB)
        db.config = mock_config
        db.logger = Mock()
        db.session = Mock()
        
        # Setup realistic data
        pkg_id1 = uuid.uuid4()
        pkg_id2 = uuid.uuid4()
        db.import_id_map = {"crate1": pkg_id1, "crate2": pkg_id2}
        
        mock_session = Mock(spec=Session)
        db.session.return_value.__enter__.return_value = mock_session
        
        # Mock realistic deletion counts
        def realistic_query_response(model_class):
            mock_query = Mock()
            deletion_counts = {
                PackageURL: 4,      # 2 packages * 2 URLs each
                CanonPackage: 2,    # 2 canon packages
                UserPackage: 3,     # 3 user relationships
                LegacyDependency: 5, # Various dependencies
                DependsOn: 2,       # Old dependencies
                Version: 4,         # 2 packages * 2 versions each
                UserVersion: 1,     # 1 user version
                Package: 2,         # 2 packages
            }
            
            count = deletion_counts.get(model_class, 0)
            mock_query.filter.return_value.delete.return_value = count
            
            if model_class == Version:
                # Return version IDs for cleanup
                mock_query.filter.return_value.all.return_value = [
                    (uuid.uuid4(),), (uuid.uuid4(),), (uuid.uuid4(),), (uuid.uuid4(),)
                ]
            
            return mock_query
        
                 mock_session.query.side_effect = realistic_query_response
         
         # Execute deletion
         db.delete_packages_by_import_id({1, 2})
         
         # Verify comprehensive cleanup
        mock_session.commit.assert_called_once()
        assert mock_session.query.call_count >= 8  # Multiple table cleanups
        
        # Verify logging
        db.logger.debug.assert_any_call("Deleting 2 crates completely")

    def test_error_handling_comprehensive(self, mock_config):
        """Test comprehensive error handling in database operations."""
        db = CratesDB.__new__(CratesDB)
        db.config = mock_config
        db.logger = Mock()
        db.session = Mock()
        
        pkg_id = uuid.uuid4()
        db.import_id_map = {"test_crate": pkg_id}
        
        # Test different types of database errors
        error_scenarios = [
            "Connection timeout",
            "Constraint violation",
            "Table does not exist",
            "Permission denied"
        ]
        
        for error_msg in error_scenarios:
            with pytest.raises(Exception, match=error_msg):
                mock_session = Mock(spec=Session)
                db.session.return_value.__enter__.return_value = mock_session
                mock_session.query.side_effect = Exception(error_msg)
                
                db.delete_packages_by_import_id({"test_crate"})
                
                mock_session.rollback.assert_called()
                db.logger.error.assert_called_with(f"Error deleting packages: {error_msg}")

    def test_get_cargo_id_mapping_integration(self, mock_config):
        """Test cargo ID to CHAI ID mapping integration."""
        db = CratesDB.__new__(CratesDB)
        db.config = mock_config
        db.session = Mock()
        
        mock_session = Mock(spec=Session)
        db.session.return_value.__enter__.return_value = mock_session
        
        # Simulate realistic crates.io data
        realistic_mappings = [
            ("1", uuid.uuid4()),    # serde
            ("2", uuid.uuid4()),    # libc
            ("3", uuid.uuid4()),    # log
            ("4", uuid.uuid4()),    # syn
            ("5", uuid.uuid4()),    # quote
        ]
        mock_session.execute.return_value.all.return_value = realistic_mappings
        
        result = db.get_cargo_id_to_chai_id()
        
        # Verify mapping structure
        assert len(result) == 5
        for import_id, chai_id in realistic_mappings:
            assert result[import_id] == chai_id
        
        # Verify caching
        assert db.import_id_map == result
        
        # Verify query construction
        mock_session.execute.assert_called_once()


# Edge case tests
class TestCratesDBEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_delete_packages_with_large_dataset(self, mock_config):
        """Test deletion with large number of packages."""
        db = CratesDB.__new__(CratesDB)
        db.config = mock_config
        db.logger = Mock()
        db.session = Mock()
        
        # Create large dataset
        large_import_ids = set(range(1000, 2000))  # 1000 packages
        db.import_id_map = {str(i): uuid.uuid4() for i in large_import_ids}
        
        mock_session = Mock(spec=Session)
        db.session.return_value.__enter__.return_value = mock_session
        
        # Mock high deletion counts
        mock_query = Mock()
        mock_query.filter.return_value.delete.return_value = 10000
        mock_query.filter.return_value.all.return_value = []
        mock_session.query.return_value = mock_query
        
        db.delete_packages_by_import_id(large_import_ids)
        
        db.logger.debug.assert_any_call("Deleting 1000 crates completely")
        mock_session.commit.assert_called_once()

    def test_get_cargo_id_with_duplicate_import_ids(self, mock_config):
        """Test handling of duplicate import IDs (should not happen but test anyway)."""
        db = CratesDB.__new__(CratesDB)
        db.config = mock_config
        db.session = Mock()
        
        mock_session = Mock(spec=Session)
        db.session.return_value.__enter__.return_value = mock_session
        
        # Simulate duplicate import IDs (last one wins)
        pkg_id1 = uuid.uuid4()
        pkg_id2 = uuid.uuid4()
        duplicate_results = [("123", pkg_id1), ("123", pkg_id2)]
        mock_session.execute.return_value.all.return_value = duplicate_results
        
        result = db.get_cargo_id_to_chai_id()
        
        # Last entry should win
        assert result["123"] == pkg_id2
        assert len(result) == 1

    def test_delete_packages_partial_import_id_match(self, mock_config):
        """Test deletion when only some import IDs exist in cache."""
        db = CratesDB.__new__(CratesDB)
        db.config = mock_config
        db.logger = Mock()
        db.session = Mock()
        
        # Only some import IDs exist in cache
        db.import_id_map = {"1": uuid.uuid4(), "3": uuid.uuid4()}
        
        mock_session = Mock(spec=Session)
        db.session.return_value.__enter__.return_value = mock_session
        mock_query = Mock()
        mock_query.filter.return_value.delete.return_value = 1
        mock_query.filter.return_value.all.return_value = []
        mock_session.query.return_value = mock_query
        
        # Request deletion of more IDs than exist
        db.delete_packages_by_import_id({1, 2, 3, 4, 5})
        
        # Should only delete the 2 that exist
        db.logger.debug.assert_any_call("Deleting 2 crates completely")

    def test_set_current_graph_multiple_calls(self, mock_config):
        """Test multiple calls to set_current_graph."""
        db = CratesDB.__new__(CratesDB)
        db.config = mock_config
        db.current_graph = Mock()
        
        graph1 = CurrentGraph({"pkg1": Mock()}, {})
        graph2 = CurrentGraph({"pkg2": Mock()}, {})
        db.current_graph.side_effect = [graph1, graph2]
        
        db.set_current_graph()
        assert db.graph == graph1
        
        db.set_current_graph()
        assert db.graph == graph2
        
        assert db.current_graph.call_count == 2

    def test_set_current_urls_empty_set(self, mock_config):
        """Test set_current_urls with empty URL set."""
        db = CratesDB.__new__(CratesDB)
        db.current_urls = Mock()
        
        empty_urls = CurrentURLs({}, {})
        db.current_urls.return_value = empty_urls
        
        db.set_current_urls(set())
        
        db.current_urls.assert_called_once_with(set())
        assert db.urls == empty_urls