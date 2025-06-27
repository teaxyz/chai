from datetime import datetime
from typing import Callable
from uuid import UUID, uuid4

import pytest

from core.models import Package
from core.structs import Cache
from package_managers.homebrew.diff import Diff
from package_managers.homebrew.structs import Actual


@pytest.fixture
def package_ids() -> dict[str, UUID]:
    """Fixture providing consistent package IDs for testing."""
    return {"foo": uuid4(), "bar": uuid4(), "baz": uuid4(), "qux": uuid4()}


@pytest.fixture
def packages(package_ids) -> dict[str, Package]:
    """Fixture providing test packages."""
    return {
        "foo": Package(
            id=package_ids["foo"],
            name="foo",
            package_manager_id=1,
            import_id="foo",
            created_at=datetime.now(),
            updated_at=datetime.now(),
        ),
        "bar": Package(
            id=package_ids["bar"],
            name="bar",
            package_manager_id=1,
            import_id="bar",
            created_at=datetime.now(),
            updated_at=datetime.now(),
        ),
        "baz": Package(
            id=package_ids["baz"],
            name="baz",
            package_manager_id=1,
            import_id="baz",
            created_at=datetime.now(),
            updated_at=datetime.now(),
        ),
        "qux": Package(
            id=package_ids["qux"],
            name="qux",
            package_manager_id=1,
            import_id="qux",
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

    def create_diff(
        package_map, dependencies=None, url_map=None, package_urls=None
    ) -> Diff:
        cache = Cache(
            package_map=package_map,
            url_map=url_map or {},
            package_urls=package_urls or {},
            dependencies=dependencies or {},
        )
        return Diff(mock_config, cache)

    return create_diff


@pytest.fixture
def homebrew_formula():
    """
    Factory fixture to create Actual homebrew formula objects.

    Returns a function that creates Actual objects.
    """

    def create_formula(
        formula_name,
        dependencies=None,
        build_dependencies=None,
        test_dependencies=None,
        recommended_dependencies=None,
        optional_dependencies=None,
    ):
        return Actual(
            formula=formula_name,
            description="Test formula",
            license="MIT",
            homepage="",
            source="",
            repository="",
            dependencies=dependencies or [],
            build_dependencies=build_dependencies or [],
            test_dependencies=test_dependencies or [],
            recommended_dependencies=recommended_dependencies or [],
            optional_dependencies=optional_dependencies or [],
        )

    return create_formula
