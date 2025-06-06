"""
Common test fixtures and configurations for pytest.

This module provides reusable fixtures for testing the CHAI package indexer.
Instead of mocking database operations, these fixtures focus on providing
test data and mock objects for testing the core logic of transformers,
parsers, and other components.
"""

import uuid
from unittest.mock import MagicMock, Mock

import pytest

from core.config import (
    Config,
    DependencyTypes,
    PackageManagers,
    PMConf,
    URLTypes,
    UserTypes,
)
from core.models import Source, URLType


@pytest.fixture
def mock_url_types():
    """
    Mock URL types with consistent UUIDs for testing.
    
    Returns a mock URLTypes object that returns consistent URL type objects
    for common URL type names.
    """
    url_types = MagicMock(spec=URLTypes)
    
    # Define URL types with consistent UUIDs
    url_type_mapping = {
        "homepage": Mock(id=uuid.UUID("00000000-0000-0000-0000-000000000001")),
        "repository": Mock(id=uuid.UUID("00000000-0000-0000-0000-000000000002")),
        "documentation": Mock(id=uuid.UUID("00000000-0000-0000-0000-000000000003")),
        "source": Mock(id=uuid.UUID("00000000-0000-0000-0000-000000000004")),
    }
    
    # Mock the __getattr__ method to return URL types by name
    url_types.__getattr__.side_effect = lambda name: url_type_mapping.get(name)
    
    return url_types


@pytest.fixture
def mock_dependency_types():
    """
    Mock dependency types for testing.
    
    Returns a mock DependencyTypes object with common dependency types.
    """
    dep_types = MagicMock(spec=DependencyTypes)
    
    # Define dependency types with consistent UUIDs
    dep_type_mapping = {
        "runtime": Mock(id=uuid.UUID("00000000-0000-0000-0000-000000000010")),
        "build": Mock(id=uuid.UUID("00000000-0000-0000-0000-000000000011")),
        "dev": Mock(id=uuid.UUID("00000000-0000-0000-0000-000000000012")),
        "test": Mock(id=uuid.UUID("00000000-0000-0000-0000-000000000013")),
    }
    
    # Mock the __getattr__ method
    dep_types.__getattr__.side_effect = lambda name: dep_type_mapping.get(name)
    
    return dep_types


@pytest.fixture
def mock_sources():
    """
    Mock sources with consistent UUIDs for testing.
    
    Returns a dict mapping source names to mock Source objects.
    """
    return {
        "github": Mock(spec=Source, id=uuid.UUID("00000000-0000-0000-0000-000000000020")),
        "crates": Mock(spec=Source, id=uuid.UUID("00000000-0000-0000-0000-000000000021")),
        "homebrew": Mock(spec=Source, id=uuid.UUID("00000000-0000-0000-0000-000000000022")),
        "debian": Mock(spec=Source, id=uuid.UUID("00000000-0000-0000-0000-000000000023")),
        "pkgx": Mock(spec=Source, id=uuid.UUID("00000000-0000-0000-0000-000000000024")),
    }


@pytest.fixture
def mock_package_managers():
    """
    Mock package managers for testing.
    
    Returns a mock PackageManagers object.
    """
    package_managers = MagicMock(spec=PackageManagers)
    
    # Define package manager mappings
    pm_mapping = {
        "crates": Mock(id=uuid.UUID("00000000-0000-0000-0000-000000000030")),
        "homebrew": Mock(id=uuid.UUID("00000000-0000-0000-0000-000000000031")),
        "debian": Mock(id=uuid.UUID("00000000-0000-0000-0000-000000000032")),
        "pkgx": Mock(id=uuid.UUID("00000000-0000-0000-0000-000000000033")),
    }
    
    # Mock the __getattr__ method
    package_managers.__getattr__.side_effect = lambda name: pm_mapping.get(name)
    
    return package_managers


@pytest.fixture
def mock_pm_config(mock_package_managers):
    """
    Mock PMConf (Package Manager Configuration) for testing.
    
    Returns a mock PMConf object with a default package manager ID.
    """
    pm_config = MagicMock(spec=PMConf)
    pm_config.pm_id = mock_package_managers.crates.id
    return pm_config


@pytest.fixture
def mock_config(mock_url_types, mock_dependency_types, mock_package_managers, mock_pm_config):
    """
    Mock Config object with all necessary sub-configurations.
    
    This is the main configuration fixture that most tests will use.
    """
    config = MagicMock(spec=Config)
    
    # Set up execution configuration
    config.exec_config = MagicMock()
    config.exec_config.test = True
    config.exec_config.no_cache = True
    config.exec_config.debug = False
    
    # Set up sub-configurations
    config.url_types = mock_url_types
    config.dependency_types = mock_dependency_types
    config.package_managers = mock_package_managers
    config.pm_config = mock_pm_config
    
    # Mock DB that returns consistent source objects
    mock_db = MagicMock()
    mock_sources_dict = {
        "github": Mock(id=uuid.UUID("00000000-0000-0000-0000-000000000020")),
        "crates": Mock(id=uuid.UUID("00000000-0000-0000-0000-000000000021")),
        "homebrew": Mock(id=uuid.UUID("00000000-0000-0000-0000-000000000022")),
        "debian": Mock(id=uuid.UUID("00000000-0000-0000-0000-000000000023")),
        "pkgx": Mock(id=uuid.UUID("00000000-0000-0000-0000-000000000024")),
    }
    
    mock_db.select_source_by_name.side_effect = lambda name: mock_sources_dict.get(name)
    mock_db.select_url_types_by_name.side_effect = lambda name: mock_url_types.__getattr__(name)
    
    config.db = mock_db
    
    return config


@pytest.fixture
def mock_user_types():
    """
    Mock user types for testing.
    
    Returns a mock UserTypes object.
    """
    user_types = MagicMock(spec=UserTypes)
    
    # Define user type mappings
    user_type_mapping = {
        "admin": Mock(id=uuid.UUID("00000000-0000-0000-0000-000000000040")),
        "maintainer": Mock(id=uuid.UUID("00000000-0000-0000-0000-000000000041")),
        "contributor": Mock(id=uuid.UUID("00000000-0000-0000-0000-000000000042")),
    }
    
    user_types.__getattr__.side_effect = lambda name: user_type_mapping.get(name)
    
    return user_types


@pytest.fixture
def sample_package_data():
    """
    Provides sample package data for testing transformers and parsers.
    
    Returns a dict with sample data for different package managers.
    """
    return {
        "crates": {
            "name": "serde",
            "version": "1.0.130",
            "description": "A generic serialization/deserialization framework",
            "homepage": "https://serde.rs",
            "repository": "https://github.com/serde-rs/serde",
            "dependencies": {
                "serde_derive": "1.0.130",
            },
        },
        "homebrew": {
            "name": "wget",
            "version": "1.21.2",
            "description": "Internet file retriever",
            "homepage": "https://www.gnu.org/software/wget/",
            "dependencies": ["gettext", "libidn2", "openssl@1.1"],
        },
        "debian": {
            "package": "curl",
            "version": "7.74.0-1.3+deb11u1",
            "maintainer": "Alessandro Ghedini <ghedo@debian.org>",
            "depends": ["libc6", "libcurl4", "zlib1g"],
        },
        "pkgx": {
            "full_name": "gnu.org/wget",
            "version": "1.21.2",
            "homepage": "https://www.gnu.org/software/wget/",
            "dependencies": {
                "gnu.org/gettext": "^0.21",
                "openssl.org": "^1.1",
            },
        },
    }


@pytest.fixture
def mock_csv_reader():
    """
    Creates a mock CSV reader for testing transformers that read CSV files.
    
    Returns a function that creates mock readers with specific data.
    """
    def create_mock_reader(data):
        """
        Create a mock reader that returns the specified data.
        
        Args:
            data: List of rows to return from the CSV reader
            
        Returns:
            A mock function that returns an iterator over the data
        """
        def mock_reader(file_key):
            return iter([data])
        
        return mock_reader
    
    return create_mock_reader


# Markers for categorizing tests
def pytest_configure(config):
    """Register custom markers for test categorization."""
    config.addinivalue_line("markers", "unit: Unit tests")
    config.addinivalue_line("markers", "integration: Integration tests")
    config.addinivalue_line("markers", "slow: Slow running tests")
    config.addinivalue_line("markers", "parser: Parser tests")
    config.addinivalue_line("markers", "transformer: Transformer tests")
    config.addinivalue_line("markers", "loader: Loader tests")
    config.addinivalue_line("markers", "ranker: Ranker tests")
