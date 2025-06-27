#!/usr/bin/env pkgx uv run
"""
Comprehensive tests for core/db.py module.

Tests cover DB class initialization, database operations, query methods,
batch operations, and ConfigDB subclass functionality.
"""

import os
import uuid
from datetime import datetime
from unittest.mock import MagicMock, Mock, patch

import pytest
from sqlalchemy import Insert, Update
from sqlalchemy.orm import Session

from core.db import DB, ConfigDB, DEFAULT_BATCH_SIZE
from core.models import (
    URL,
    LegacyDependency,
    LoadHistory,
    Package,
    PackageManager,
    PackageURL,
    Source,
    URLType,
    DependsOnType,
)
from core.structs import CurrentGraph, CurrentURLs, URLKey


class TestDB:
    """Test cases for the DB class."""

    @patch("core.db.create_engine")
    @patch("core.db.sessionmaker")
    def test_db_init(self, mock_sessionmaker, mock_create_engine, mock_logger):
        """Test DB initialization with proper setup."""
        mock_engine = Mock()
        mock_create_engine.return_value = mock_engine
        mock_session_factory = Mock()
        mock_sessionmaker.return_value = mock_session_factory

        with patch.dict(os.environ, {"CHAI_DATABASE_URL": "postgresql://test"}):
            db = DB("test_db")

        mock_create_engine.assert_called_once_with("postgresql://test")
        mock_sessionmaker.assert_called_once_with(mock_engine)
        assert db.engine == mock_engine
        assert db.session == mock_session_factory
        assert isinstance(db.now, datetime)

    @patch("core.db.create_engine")
    @patch("core.db.sessionmaker")
    def test_db_init_no_database_url(self, mock_sessionmaker, mock_create_engine):
        """Test DB initialization when CHAI_DATABASE_URL is not set."""
        with patch.dict(os.environ, {}, clear=True):
            # Should use None as default when env var is not set
            db = DB("test_db")
            mock_create_engine.assert_called_once_with(None)

    @patch("core.db.create_engine")
    @patch("core.db.sessionmaker")
    def test_insert_load_history(self, mock_sessionmaker, mock_create_engine):
        """Test inserting load history record."""
        mock_session = Mock()
        mock_sessionmaker.return_value.return_value.__enter__.return_value = mock_session

        db = DB("test_db")
        package_manager_id = str(uuid.uuid4())
        
        db.insert_load_history(package_manager_id)

        mock_session.add.assert_called_once()
        added_obj = mock_session.add.call_args[0][0]
        assert isinstance(added_obj, LoadHistory)
        assert added_obj.package_manager_id == package_manager_id
        mock_session.commit.assert_called_once()

    @patch("core.db.create_engine")
    @patch("core.db.sessionmaker")
    def test_print_statement(self, mock_sessionmaker, mock_create_engine):
        """Test print_statement method with SQL statement compilation."""
        db = DB("test_db")
        mock_stmt = Mock()
        mock_compiled = Mock()
        mock_stmt.compile.return_value = mock_compiled
        mock_compiled.__str__ = Mock(return_value="SELECT * FROM test")

        db.print_statement(mock_stmt)

        mock_stmt.compile.assert_called_once()
        # Logger should be called with the compiled statement string
        assert db.logger.log.called

    @patch("core.db.create_engine")
    @patch("core.db.sessionmaker")
    def test_close(self, mock_sessionmaker, mock_create_engine):
        """Test database connection cleanup."""
        mock_engine = Mock()
        mock_create_engine.return_value = mock_engine

        db = DB("test_db")
        db.close()

        mock_engine.dispose.assert_called_once()

    @patch("core.db.create_engine")
    @patch("core.db.sessionmaker")
    def test_search_names(self, mock_sessionmaker, mock_create_engine):
        """Test searching for package names and returning homepage URLs."""
        mock_session = Mock()
        mock_sessionmaker.return_value.return_value.__enter__.return_value = mock_session

        # Mock query results
        mock_package = Mock()
        mock_package.name = "test-package"
        mock_url = Mock()
        mock_url.url = "https://example.com"
        
        mock_result = Mock()
        mock_result.Package = mock_package
        mock_result.URL = mock_url
        
        mock_session.query.return_value.join.return_value.join.return_value.join.return_value.filter.return_value.filter.return_value.filter.return_value.all.return_value = [mock_result]

        db = DB("test_db")
        package_names = ["test-package", "another-package"]
        package_managers = [uuid.uuid4(), uuid.uuid4()]
        
        result = db.search_names(package_names, package_managers)

        assert result == ["https://example.com"]
        # Verify the query was built correctly
        assert mock_session.query.called

    @patch("core.db.create_engine")
    @patch("core.db.sessionmaker")
    def test_search_names_empty_result(self, mock_sessionmaker, mock_create_engine):
        """Test search_names with no matching packages."""
        mock_session = Mock()
        mock_sessionmaker.return_value.return_value.__enter__.return_value = mock_session
        mock_session.query.return_value.join.return_value.join.return_value.join.return_value.filter.return_value.filter.return_value.filter.return_value.all.return_value = []

        db = DB("test_db")
        result = db.search_names(["nonexistent"], [uuid.uuid4()])

        assert result == []

    @patch("core.db.create_engine")
    @patch("core.db.sessionmaker")
    def test_current_graph(self, mock_sessionmaker, mock_create_engine):
        """Test retrieving current package graph for a package manager."""
        mock_session = Mock()
        mock_sessionmaker.return_value.return_value.__enter__.return_value = mock_session

        # Mock package and dependency data
        pkg_id = uuid.uuid4()
        mock_package = Package(id=pkg_id, import_id="test-package")
        mock_dependency = LegacyDependency(package_id=pkg_id, name="dep1")
        
        mock_session.execute.return_value = [(mock_package, mock_dependency)]

        db = DB("test_db")
        package_manager_id = uuid.uuid4()
        
        result = db.current_graph(package_manager_id)

        assert isinstance(result, CurrentGraph)
        assert "test-package" in result.package_map
        assert result.package_map["test-package"] == mock_package
        assert pkg_id in result.dependencies
        assert mock_dependency in result.dependencies[pkg_id]

    @patch("core.db.create_engine")
    @patch("core.db.sessionmaker")
    def test_current_graph_no_dependencies(self, mock_sessionmaker, mock_create_engine):
        """Test current_graph with packages that have no dependencies."""
        mock_session = Mock()
        mock_sessionmaker.return_value.return_value.__enter__.return_value = mock_session

        pkg_id = uuid.uuid4()
        mock_package = Package(id=pkg_id, import_id="test-package")
        
        # None dependency indicates outer join with no match
        mock_session.execute.return_value = [(mock_package, None)]

        db = DB("test_db")
        result = db.current_graph(uuid.uuid4())

        assert isinstance(result, CurrentGraph)
        assert "test-package" in result.package_map
        # No dependencies should be added for None values
        assert pkg_id not in result.dependencies or len(result.dependencies[pkg_id]) == 0

    def test_build_current_urls(self):
        """Test _build_current_urls helper method."""
        db = DB.__new__(DB)  # Create instance without __init__
        db.logger = Mock()

        # Mock query result data
        pkg_id = uuid.uuid4()
        url_id = uuid.uuid4()
        url_type_id = uuid.uuid4()
        
        mock_package = Package(id=pkg_id)
        mock_url = URL(id=url_id, url="https://example.com", url_type_id=url_type_id)
        mock_package_url = PackageURL(package_id=pkg_id, url_id=url_id)
        
        mock_result = [(mock_package, mock_package_url, mock_url)]
        
        result = db._build_current_urls(mock_result)
        
        assert isinstance(result, CurrentURLs)
        url_key = URLKey("https://example.com", url_type_id)
        assert url_key in result.url_map
        assert result.url_map[url_key] == mock_url
        assert pkg_id in result.package_urls
        assert mock_package_url in result.package_urls[pkg_id]

    def test_build_current_urls_with_none_package(self):
        """Test _build_current_urls with None package (outer join case)."""
        db = DB.__new__(DB)  # Create instance without __init__
        db.logger = Mock()

        url_id = uuid.uuid4()
        url_type_id = uuid.uuid4()
        mock_url = URL(id=url_id, url="https://example.com", url_type_id=url_type_id)
        
        # Package is None due to outer join
        mock_result = [(None, None, mock_url)]
        
        result = db._build_current_urls(mock_result)
        
        assert isinstance(result, CurrentURLs)
        url_key = URLKey("https://example.com", url_type_id)
        assert url_key in result.url_map
        assert len(result.package_urls) == 0

    @patch("core.db.create_engine")
    @patch("core.db.sessionmaker")
    def test_current_urls(self, mock_sessionmaker, mock_create_engine):
        """Test retrieving current URLs for a specific set of URLs."""
        mock_session = Mock()
        mock_sessionmaker.return_value.return_value.__enter__.return_value = mock_session
        
        # Mock the execute result
        mock_result = []
        mock_session.execute.return_value = mock_result

        db = DB("test_db")
        with patch.object(db, '_build_current_urls') as mock_build:
            mock_current_urls = CurrentURLs({}, {})
            mock_build.return_value = mock_current_urls
            
            urls = {"https://example.com", "https://test.com"}
            result = db.current_urls(urls)
            
            assert result == mock_current_urls
            mock_build.assert_called_once_with(mock_result)

    @patch("core.db.create_engine")
    @patch("core.db.sessionmaker")
    def test_all_current_urls(self, mock_sessionmaker, mock_create_engine):
        """Test retrieving all current URLs from the database."""
        mock_session = Mock()
        mock_sessionmaker.return_value.return_value.__enter__.return_value = mock_session
        
        mock_result = []
        mock_session.execute.return_value = mock_result

        db = DB("test_db")
        with patch.object(db, '_build_current_urls') as mock_build:
            mock_current_urls = CurrentURLs({}, {})
            mock_build.return_value = mock_current_urls
            
            result = db.all_current_urls()
            
            assert result == mock_current_urls
            mock_build.assert_called_once_with(mock_result)

    @patch("core.db.create_engine")
    @patch("core.db.sessionmaker")
    def test_load_with_data(self, mock_sessionmaker, mock_create_engine):
        """Test load method with data."""
        db = DB("test_db")
        mock_session = Mock()
        mock_stmt = Mock()
        
        # Mock objects with to_dict_v2 method
        mock_obj1 = Mock()
        mock_obj1.to_dict_v2.return_value = {"id": "1", "name": "test1"}
        mock_obj2 = Mock()
        mock_obj2.to_dict_v2.return_value = {"id": "2", "name": "test2"}
        
        data = [mock_obj1, mock_obj2]
        
        with patch.object(db, 'batch') as mock_batch:
            db.load(mock_session, data, mock_stmt)
            
            expected_values = [{"id": "1", "name": "test1"}, {"id": "2", "name": "test2"}]
            mock_batch.assert_called_once_with(mock_session, mock_stmt, expected_values, DEFAULT_BATCH_SIZE)

    @patch("core.db.create_engine")
    @patch("core.db.sessionmaker")
    def test_load_empty_data(self, mock_sessionmaker, mock_create_engine):
        """Test load method with empty data."""
        db = DB("test_db")
        mock_session = Mock()
        mock_stmt = Mock()
        
        with patch.object(db, 'batch') as mock_batch:
            db.load(mock_session, [], mock_stmt)
            
            # Should not call batch with empty data
            mock_batch.assert_not_called()

    @patch("core.db.create_engine")
    @patch("core.db.sessionmaker")
    def test_batch(self, mock_sessionmaker, mock_create_engine):
        """Test batch processing of database operations."""
        db = DB("test_db")
        mock_session = Mock()
        mock_stmt = Mock()
        
        # Test data that would require multiple batches
        values = [{"id": str(i)} for i in range(15000)]  # Larger than DEFAULT_BATCH_SIZE
        
        db.batch(mock_session, mock_stmt, values, 10000)
        
        # Should be called twice due to batch size
        assert mock_session.execute.call_count == 2

    @patch("core.db.create_engine")
    @patch("core.db.sessionmaker")
    def test_batch_single_batch(self, mock_sessionmaker, mock_create_engine):
        """Test batch processing with data that fits in one batch."""
        db = DB("test_db")
        mock_session = Mock()
        mock_stmt = Mock()
        
        values = [{"id": "1"}, {"id": "2"}]
        
        db.batch(mock_session, mock_stmt, values, 10000)
        
        # Should be called once for small data
        mock_session.execute.assert_called_once()

    @patch("core.db.create_engine")
    @patch("core.db.sessionmaker")
    def test_ingest_success(self, mock_sessionmaker, mock_create_engine):
        """Test successful ingest operation."""
        mock_session = Mock()
        mock_sessionmaker.return_value.return_value.__enter__.return_value = mock_session

        db = DB("test_db")
        
        # Mock data
        new_packages = [Package(name="pkg1")]
        new_urls = [URL(url="https://example.com")]
        new_package_urls = [PackageURL()]
        new_deps = [LegacyDependency(name="dep1")]
        removed_deps = [LegacyDependency(name="old_dep")]
        updated_packages = [{"id": uuid.uuid4(), "name": "updated_pkg"}]
        updated_package_urls = [{"id": uuid.uuid4(), "url_id": uuid.uuid4()}]
        
        with patch.object(db, 'execute') as mock_execute:
            db.ingest(new_packages, new_urls, new_package_urls, new_deps, 
                     removed_deps, updated_packages, updated_package_urls)
            
            # Verify execute was called for each type of operation
            assert mock_execute.call_count == 5
            mock_session.commit.assert_called_once()

    @patch("core.db.create_engine")
    @patch("core.db.sessionmaker")
    def test_ingest_failure_rollback(self, mock_sessionmaker, mock_create_engine):
        """Test ingest operation failure and rollback."""
        mock_session = Mock()
        mock_sessionmaker.return_value.return_value.__enter__.return_value = mock_session
        mock_session.execute.side_effect = Exception("Database error")

        db = DB("test_db")
        
        new_packages = [Package(name="pkg1")]
        
        with patch.object(db, 'execute'):
            with pytest.raises(Exception, match="Database error"):
                db.ingest(new_packages, [], [], [], [], [], [])
            
            mock_session.rollback.assert_called_once()

    @patch("core.db.create_engine")
    @patch("core.db.sessionmaker")
    def test_execute_add(self, mock_sessionmaker, mock_create_engine):
        """Test execute method with add operation."""
        db = DB("test_db")
        mock_session = Mock()
        
        data = [Package(name="pkg1"), Package(name="pkg2")]
        
        db.execute(mock_session, data, "add", "test packages")
        
        mock_session.add_all.assert_called_once_with(data)
        mock_session.flush.assert_called_once()

    @patch("core.db.create_engine")
    @patch("core.db.sessionmaker")
    def test_execute_delete(self, mock_sessionmaker, mock_create_engine):
        """Test execute method with delete operation."""
        db = DB("test_db")
        mock_session = Mock()
        
        data = [Package(name="pkg1")]
        
        with patch.object(db, 'remove_all') as mock_remove:
            db.execute(mock_session, data, "delete", "test packages")
            
            mock_remove.assert_called_once_with(mock_session, data)
            mock_session.flush.assert_called_once()

    @patch("core.db.create_engine")
    @patch("core.db.sessionmaker")
    def test_execute_invalid_method(self, mock_sessionmaker, mock_create_engine):
        """Test execute method with invalid operation."""
        db = DB("test_db")
        mock_session = Mock()
        
        with pytest.raises(ValueError, match="db.execute\\(invalid\\) is unknown"):
            db.execute(mock_session, [], "invalid", "test")

    @patch("core.db.create_engine")
    @patch("core.db.sessionmaker")
    def test_execute_empty_data(self, mock_sessionmaker, mock_create_engine):
        """Test execute method with empty data."""
        db = DB("test_db")
        mock_session = Mock()
        
        db.execute(mock_session, [], "add", "empty data")
        
        # Should not call session methods with empty data
        mock_session.add_all.assert_not_called()
        mock_session.flush.assert_not_called()

    @patch("core.db.create_engine")
    @patch("core.db.sessionmaker")
    def test_remove_all(self, mock_sessionmaker, mock_create_engine):
        """Test remove_all method."""
        db = DB("test_db")
        mock_session = Mock()
        
        data = [Package(name="pkg1"), Package(name="pkg2")]
        
        db.remove_all(mock_session, data)
        
        # Should call delete for each item
        assert mock_session.delete.call_count == len(data)


class TestConfigDB:
    """Test cases for the ConfigDB class."""

    @patch("core.db.create_engine")
    @patch("core.db.sessionmaker")
    def test_select_package_manager_by_name_found(self, mock_sessionmaker, mock_create_engine):
        """Test selecting package manager by name when found."""
        mock_session = Mock()
        mock_sessionmaker.return_value.return_value.__enter__.return_value = mock_session
        
        mock_pm = PackageManager(id=uuid.uuid4(), source_id=uuid.uuid4())
        mock_session.query.return_value.join.return_value.filter.return_value.first.return_value = mock_pm

        config_db = ConfigDB()
        result = config_db.select_package_manager_by_name("crates")
        
        assert result == mock_pm

    @patch("core.db.create_engine")
    @patch("core.db.sessionmaker")
    def test_select_package_manager_by_name_not_found(self, mock_sessionmaker, mock_create_engine):
        """Test selecting package manager by name when not found."""
        mock_session = Mock()
        mock_sessionmaker.return_value.return_value.__enter__.return_value = mock_session
        mock_session.query.return_value.join.return_value.filter.return_value.first.return_value = None

        config_db = ConfigDB()
        
        with pytest.raises(ValueError, match="Package manager nonexistent not found"):
            config_db.select_package_manager_by_name("nonexistent")

    @patch("core.db.create_engine")
    @patch("core.db.sessionmaker")
    def test_select_url_types_by_name(self, mock_sessionmaker, mock_create_engine):
        """Test selecting URL type by name."""
        mock_session = Mock()
        mock_sessionmaker.return_value.return_value.__enter__.return_value = mock_session
        
        mock_url_type = URLType(id=uuid.uuid4(), name="homepage")
        mock_session.query.return_value.filter.return_value.first.return_value = mock_url_type

        config_db = ConfigDB()
        result = config_db.select_url_types_by_name("homepage")
        
        assert result == mock_url_type

    @patch("core.db.create_engine")
    @patch("core.db.sessionmaker")  
    def test_select_source_by_name(self, mock_sessionmaker, mock_create_engine):
        """Test selecting source by name."""
        mock_session = Mock()
        mock_sessionmaker.return_value.return_value.__enter__.return_value = mock_session
        
        mock_source = Source(id=uuid.uuid4(), type="github")
        mock_session.query.return_value.filter.return_value.first.return_value = mock_source

        config_db = ConfigDB()
        result = config_db.select_source_by_name("github")
        
        assert result == mock_source

    @patch("core.db.create_engine")
    @patch("core.db.sessionmaker")
    def test_select_dependency_type_by_name(self, mock_sessionmaker, mock_create_engine):
        """Test selecting dependency type by name."""
        mock_session = Mock()
        mock_sessionmaker.return_value.return_value.__enter__.return_value = mock_session
        
        mock_dep_type = DependsOnType(id=uuid.uuid4(), name="runtime")
        mock_session.query.return_value.filter.return_value.first.return_value = mock_dep_type

        config_db = ConfigDB()
        result = config_db.select_dependency_type_by_name("runtime")
        
        assert result == mock_dep_type


@pytest.fixture
def mock_logger():
    """Mock logger for DB tests."""
    return Mock()