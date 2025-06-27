from datetime import datetime
from uuid import uuid4

import pytest

from core.models import Package
from core.structs import Cache
from package_managers.crates.main import Diff
from package_managers.crates.structs import (
    Crate,
    CrateLatestVersion,
)


@pytest.fixture
def package_ids():
    """Fixture providing consistent package IDs for testing."""
    return {"main": uuid4(), "dep": uuid4()}


@pytest.fixture
def packages(package_ids):
    """Fixture providing test packages."""
    return {
        "main": Package(
            id=package_ids["main"],
            name="main_pkg",
            package_manager_id=1,
            import_id="1048221",
            created_at=datetime.now(),
            updated_at=datetime.now(),
        ),
        "dep": Package(
            id=package_ids["dep"],
            name="dep_pkg",
            package_manager_id=1,
            import_id="271975",
            created_at=datetime.now(),
            updated_at=datetime.now(),
        ),
    }


@pytest.fixture
def diff_instance(mock_config):
    """
    Factory fixture to create Diff instances with specific cache configurations.

    Returns a function that creates Diff instances.
    """

    def create_diff(package_map, dependencies=None, url_map=None, package_urls=None):
        cache = Cache(
            package_map=package_map,
            url_map=url_map or {},
            package_urls=package_urls or {},
            dependencies=dependencies or {},
        )
        return Diff(mock_config, cache)

    return create_diff


@pytest.fixture
def crate_with_dependencies():
    """
    Factory fixture to create Crate objects with specified dependencies.

    Returns a function that creates Crate objects.
    """

    def create_crate(crate_id="1048221", dependencies=None):
        latest_version = CrateLatestVersion(
            id=9337571,
            checksum="some-checksum",
            downloads=1000,
            license="MIT",
            num="1.0.0",
            published_by=None,
            published_at="2023-01-01",
        )

        if dependencies:
            latest_version.dependencies = dependencies
        else:
            latest_version.dependencies = []

        crate = Crate(
            id=int(crate_id),
            name="main_pkg",
            readme="Test readme",
            homepage="",
            repository="",
            documentation="",
            source=None,
        )
        crate.latest_version = latest_version

        return crate

    return create_crate
