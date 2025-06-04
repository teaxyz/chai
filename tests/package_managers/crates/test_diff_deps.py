import unittest
from datetime import datetime
from unittest.mock import MagicMock
from uuid import uuid4

from core.config import Config, DependencyTypes
from core.models import LegacyDependency, Package
from core.structs import Cache
from package_managers.crates.main_v2 import Diff
from package_managers.crates.structs import (
    Crate,
    CrateDependency,
    CrateLatestVersion,
    DependencyType,
)


class TestDiffDeps(unittest.TestCase):
    """Tests for the diff_deps method in the Diff class for crates."""

    def setUp(self):
        """Set up common test data and mocks."""
        # Create fixed UUIDs for dependency types for consistent testing
        self.runtime_type_id = uuid4()
        self.build_type_id = uuid4()
        self.development_type_id = uuid4()

        # Mock dependency types with predefined UUIDs
        self.mock_dep_types = MagicMock(spec=DependencyTypes)
        self.mock_dep_types.runtime = self.runtime_type_id
        self.mock_dep_types.build = self.build_type_id
        self.mock_dep_types.development = self.development_type_id

        # Create a mock Config that returns our mock dependency types
        self.mock_config = MagicMock(spec=Config)
        self.mock_config.dependency_types = self.mock_dep_types

        # Common package IDs for testing
        self.main_pkg_id = uuid4()
        self.dep_pkg_id = uuid4()

        # Common package objects
        self.main_pkg = Package(
            id=self.main_pkg_id,
            name="main_pkg",
            package_manager_id=1,
            import_id="1048221",  # As mentioned in the example
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )

        self.dep_pkg = Package(
            id=self.dep_pkg_id,
            name="dep_pkg",
            package_manager_id=1,
            import_id="271975",  # As mentioned in the example
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )

    def test_existing_dependency_no_changes(self):
        """
        Test that when a dependency already exists in the database and also appears in
        the crate object, it is neither added to new_deps nor removed_deps.
        """
        # arrange
        # Create an existing dependency between main_pkg and dep_pkg
        existing_dep = LegacyDependency(
            id=1,  # ID doesn't matter for the test
            package_id=self.main_pkg_id,
            dependency_id=self.dep_pkg_id,
            dependency_type_id=self.runtime_type_id,
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )

        # Create cache with existing package and dependency
        cache = Cache(
            package_map={
                "1048221": self.main_pkg,  # Main package
                "271975": self.dep_pkg,  # Dependency package
            },
            url_map={},
            package_urls={},
            dependencies={self.main_pkg_id: {existing_dep}},
        )

        # Create Diff instance with our mock config and cache
        diff = Diff(self.mock_config, cache)

        # Create a CrateLatestVersion with the dependency
        latest_version = CrateLatestVersion(
            id=9337571,  # Version ID from example
            checksum="some-checksum",
            downloads=1000,
            license="MIT",
            num="1.0.0",
            published_by=None,
            published_at="2023-01-01",
        )

        # Add dependency to the latest version
        dependency = CrateDependency(
            crate_id=1048221,  # Main package ID
            dependency_id=271975,  # Dependency package ID
            dependency_type=DependencyType.NORMAL,  # Normal = runtime dependency
            semver_range="^0.26.1",  # Version requirement
        )
        latest_version.dependencies = [dependency]

        # Create a Crate with the latest version
        crate = Crate(
            id=1048221,
            name="main_pkg",
            readme="Test readme",
            homepage="",
            repository="",
            documentation="",
            source=None,
        )
        crate.latest_version = latest_version

        # act
        new_deps, removed_deps = diff.diff_deps(crate)

        # assert
        self.assertEqual(len(new_deps), 0, "No new deps should be added")
        self.assertEqual(len(removed_deps), 0, "No deps should be removed")

    def test_dependency_changed_type(self):
        """
        Test that when a dependency exists but its type changes, it is both
        added to new_deps and removed_deps.
        """
        # arrange
        # Create an existing dependency between main_pkg and dep_pkg
        existing_dep = LegacyDependency(
            id=1,
            package_id=self.main_pkg_id,
            dependency_id=self.dep_pkg_id,
            dependency_type_id=self.build_type_id,  # BUILD type
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )

        # Create cache with existing package and dependency
        cache = Cache(
            package_map={
                "1048221": self.main_pkg,  # Main package
                "271975": self.dep_pkg,  # Dependency package
            },
            url_map={},
            package_urls={},
            dependencies={self.main_pkg_id: {existing_dep}},
        )

        # Create Diff instance with our mock config and cache
        diff = Diff(self.mock_config, cache)

        # Create a CrateLatestVersion with the dependency
        latest_version = CrateLatestVersion(
            id=9337571,
            checksum="some-checksum",
            downloads=1000,
            license="MIT",
            num="1.0.0",
            published_by=None,
            published_at="2023-01-01",
        )

        # Add dependency to the latest version with NORMAL type (runtime)
        dependency = CrateDependency(
            crate_id=1048221,
            dependency_id=271975,
            dependency_type=DependencyType.NORMAL,  # Changed from BUILD to NORMAL
            semver_range="^0.26.1",
        )
        latest_version.dependencies = [dependency]

        # Create a Crate with the latest version
        crate = Crate(
            id=1048221,
            name="main_pkg",
            readme="Test readme",
            homepage="",
            repository="",
            documentation="",
            source=None,
        )
        crate.latest_version = latest_version

        # act
        new_deps, removed_deps = diff.diff_deps(crate)

        # assert
        self.assertEqual(len(new_deps), 1, "One new dep should be added (new type)")
        self.assertEqual(len(removed_deps), 1, "One dep should be removed (old type)")

        # new dep should be a runtime dep
        new_dep = new_deps[0]
        self.assertEqual(new_dep.package_id, self.main_pkg_id)
        self.assertEqual(new_dep.dependency_id, self.dep_pkg_id)
        self.assertEqual(new_dep.dependency_type_id, self.runtime_type_id)

        # removed dep should be a build dep
        removed_dep = removed_deps[0]
        self.assertEqual(removed_dep.package_id, self.main_pkg_id)
        self.assertEqual(removed_dep.dependency_id, self.dep_pkg_id)
        self.assertEqual(removed_dep.dependency_type_id, self.build_type_id)

    def test_new_dependency(self):
        """
        Test that when a dependency doesn't exist in the cache but appears in the
        crate object, it is added to new_deps.
        """
        # arrange
        # Create cache with existing packages but no dependencies
        cache = Cache(
            package_map={
                "1048221": self.main_pkg,  # Main package
                "271975": self.dep_pkg,  # Dependency package
            },
            url_map={},
            package_urls={},
            dependencies={},
        )

        # Create Diff instance with our mock config and cache
        diff = Diff(self.mock_config, cache)

        # Create a CrateLatestVersion with the dependency
        latest_version = CrateLatestVersion(
            id=9337571,
            checksum="some-checksum",
            downloads=1000,
            license="MIT",
            num="1.0.0",
            published_by=None,
            published_at="2023-01-01",
        )

        # Add dependency to the latest version
        dependency = CrateDependency(
            crate_id=1048221,
            dependency_id=271975,
            dependency_type=DependencyType.NORMAL,
            semver_range="^0.26.1",
        )
        latest_version.dependencies = [dependency]

        # Create a Crate with the latest version
        crate = Crate(
            id=1048221,
            name="main_pkg",
            readme="Test readme",
            homepage="",
            repository="",
            documentation="",
            source=None,
        )
        crate.latest_version = latest_version

        # act
        new_deps, removed_deps = diff.diff_deps(crate)

        # assert
        self.assertEqual(len(new_deps), 1, "One new dep should be added")
        self.assertEqual(len(removed_deps), 0, "No deps should be removed")

        # New dep should be a runtime dep
        new_dep = new_deps[0]
        self.assertEqual(new_dep.package_id, self.main_pkg_id)
        self.assertEqual(new_dep.dependency_id, self.dep_pkg_id)
        self.assertEqual(new_dep.dependency_type_id, self.runtime_type_id)

    def test_removed_dependency(self):
        """
        Test that when a dependency exists in the cache but doesn't appear in the
        crate object, it is added to removed_deps.
        """
        # arrange
        # Create an existing dependency between main_pkg and dep_pkg
        existing_dep = LegacyDependency(
            id=1,
            package_id=self.main_pkg_id,
            dependency_id=self.dep_pkg_id,
            dependency_type_id=self.runtime_type_id,
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )

        # Create cache with existing package and dependency
        cache = Cache(
            package_map={
                "1048221": self.main_pkg,  # Main package
                "271975": self.dep_pkg,  # Dependency package
            },
            url_map={},
            package_urls={},
            dependencies={self.main_pkg_id: {existing_dep}},
        )

        # Create Diff instance with our mock config and cache
        diff = Diff(self.mock_config, cache)

        # Create a CrateLatestVersion with no dependencies
        latest_version = CrateLatestVersion(
            id=9337571,
            checksum="some-checksum",
            downloads=1000,
            license="MIT",
            num="1.0.0",
            published_by=None,
            published_at="2023-01-01",
        )

        # Empty dependencies list
        latest_version.dependencies = []

        # Create a Crate with the latest version
        crate = Crate(
            id=1048221,
            name="main_pkg",
            readme="Test readme",
            homepage="",
            repository="",
            documentation="",
            source=None,
        )
        crate.latest_version = latest_version

        # act
        new_deps, removed_deps = diff.diff_deps(crate)

        # assert
        self.assertEqual(len(new_deps), 0, "No new deps should be added")
        self.assertEqual(len(removed_deps), 1, "One dep should be removed")

        # removed dep should be a runtime dep
        removed_dep = removed_deps[0]
        self.assertEqual(removed_dep.package_id, self.main_pkg_id)
        self.assertEqual(removed_dep.dependency_id, self.dep_pkg_id)
        self.assertEqual(removed_dep.dependency_type_id, self.runtime_type_id)

    def test_multiple_dependency_types_same_package(self):
        """
        Test that when a package depends on the same dependency package with
        multiple dependency types (e.g., both runtime and build), we handle
        the unique constraint on (package_id, dependency_id) properly.

        This test should expose the current bug where we try to create multiple
        LegacyDependency records with the same package_id and dependency_id
        but different dependency_type_id, which violates the DB constraint.
        """
        # arrange
        # Create cache with existing packages but no dependencies
        cache = Cache(
            package_map={
                "1048221": self.main_pkg,  # Main package
                "271975": self.dep_pkg,  # Dependency package
            },
            url_map={},
            package_urls={},
            dependencies={},
        )

        # Create Diff instance with our mock config and cache
        diff = Diff(self.mock_config, cache)

        # Create a CrateLatestVersion with multiple dependencies to the same package
        latest_version = CrateLatestVersion(
            id=9337571,
            checksum="some-checksum",
            downloads=1000,
            license="MIT",
            num="1.0.0",
            published_by=None,
            published_at="2023-01-01",
        )

        # Add dependencies to the same package with different types
        runtime_dependency = CrateDependency(
            crate_id=1048221,
            dependency_id=271975,
            dependency_type=DependencyType.NORMAL,  # Runtime dependency
            semver_range="^0.26.1",
        )

        build_dependency = CrateDependency(
            crate_id=1048221,
            dependency_id=271975,  # Same dependency package
            dependency_type=DependencyType.BUILD,  # Build dependency
            semver_range="^0.26.1",
        )

        latest_version.dependencies = [runtime_dependency, build_dependency]

        # Create a Crate with the latest version
        crate = Crate(
            id=1048221,
            name="main_pkg",
            readme="Test readme",
            homepage="",
            repository="",
            documentation="",
            source=None,
        )
        crate.latest_version = latest_version

        # act
        new_deps, removed_deps = diff.diff_deps(crate)

        # assert
        self.assertEqual(len(removed_deps), 0, "No deps should be removed")

        # With the fix, we should only create 1 dependency record with the
        # highest priority type (NORMAL > BUILD > DEV)
        # Since we have both NORMAL and BUILD, NORMAL should win
        self.assertEqual(
            len(new_deps), 1, "Should create only 1 dep with highest priority type"
        )

        # The dependency should have the runtime type (NORMAL has highest priority)
        new_dep = new_deps[0]
        self.assertEqual(new_dep.package_id, self.main_pkg_id)
        self.assertEqual(new_dep.dependency_id, self.dep_pkg_id)
        self.assertEqual(
            new_dep.dependency_type_id,
            self.runtime_type_id,
            "Should choose NORMAL (runtime) over BUILD as it has higher priority",
        )

    def test_multiple_dependency_types_build_vs_dev(self):
        """
        Test that when a package depends on the same dependency package with
        BUILD and DEV types (no NORMAL), BUILD type takes precedence.

        Priority order: NORMAL > BUILD > DEV
        """
        # arrange
        # Create cache with existing packages but no dependencies
        cache = Cache(
            package_map={
                "1048221": self.main_pkg,  # Main package
                "271975": self.dep_pkg,  # Dependency package
            },
            url_map={},
            package_urls={},
            dependencies={},
        )

        # Create Diff instance with our mock config and cache
        diff = Diff(self.mock_config, cache)

        # Create a CrateLatestVersion with BUILD and DEV dependencies to the same package
        latest_version = CrateLatestVersion(
            id=9337571,
            checksum="some-checksum",
            downloads=1000,
            license="MIT",
            num="1.0.0",
            published_by=None,
            published_at="2023-01-01",
        )

        # Add dependencies to the same package with BUILD and DEV types
        build_dependency = CrateDependency(
            crate_id=1048221,
            dependency_id=271975,
            dependency_type=DependencyType.BUILD,  # Build dependency
            semver_range="^0.26.1",
        )

        dev_dependency = CrateDependency(
            crate_id=1048221,
            dependency_id=271975,  # Same dependency package
            dependency_type=DependencyType.DEV,  # Dev dependency
            semver_range="^0.26.1",
        )

        latest_version.dependencies = [
            dev_dependency,
            build_dependency,
        ]  # DEV first to test ordering

        # Create a Crate with the latest version
        crate = Crate(
            id=1048221,
            name="main_pkg",
            readme="Test readme",
            homepage="",
            repository="",
            documentation="",
            source=None,
        )
        crate.latest_version = latest_version

        # act
        new_deps, removed_deps = diff.diff_deps(crate)

        # assert
        self.assertEqual(len(removed_deps), 0, "No deps should be removed")
        self.assertEqual(
            len(new_deps), 1, "Should create only 1 dep with highest priority type"
        )

        # The dependency should have the build type (BUILD > DEV)
        new_dep = new_deps[0]
        self.assertEqual(new_dep.package_id, self.main_pkg_id)
        self.assertEqual(new_dep.dependency_id, self.dep_pkg_id)
        self.assertEqual(
            new_dep.dependency_type_id,
            self.build_type_id,
            "Should choose BUILD over DEV as it has higher priority",
        )
