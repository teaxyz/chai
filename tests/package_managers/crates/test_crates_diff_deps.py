"""
Test the diff_deps functionality for the crates package manager.

This module tests the Diff.diff_deps method which determines what dependencies
need to be added or removed when processing crate updates.
"""

from datetime import datetime

import pytest

from core.models import LegacyDependency
from package_managers.crates.structs import CrateDependency, DependencyType


@pytest.mark.transformer
class TestDiffDeps:
    """Tests for the diff_deps method in the Diff class for crates."""

    def test_existing_dependency_no_changes(
        self, packages, package_ids, diff_instance, crate_with_dependencies, mock_config
    ):
        """
        Test that when a dependency already exists in the database and also appears in
        the crate object, it is neither added to new_deps nor removed_deps.
        """
        # Create an existing runtime dependency
        existing_dep = LegacyDependency(
            id=1,
            package_id=package_ids["main"],
            dependency_id=package_ids["dep"],
            dependency_type_id=mock_config.dependency_types.runtime,
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )

        # Create diff with existing dependency
        diff = diff_instance(
            package_map={"1048221": packages["main"], "271975": packages["dep"]},
            dependencies={package_ids["main"]: {existing_dep}},
        )

        # Create crate with the same dependency
        dependency = CrateDependency(
            crate_id=1048221,
            dependency_id=271975,
            dependency_type=DependencyType.NORMAL,  # Runtime dependency
            semver_range="^0.26.1",
        )
        crate = crate_with_dependencies(dependencies=[dependency])

        # Execute
        new_deps, removed_deps = diff.diff_deps(crate)

        # Assert
        assert len(new_deps) == 0, "No new deps should be added"
        assert len(removed_deps) == 0, "No deps should be removed"

    def test_dependency_changed_type(
        self, packages, package_ids, diff_instance, crate_with_dependencies, mock_config
    ):
        """
        Test that when a dependency exists but its type changes, it is both
        added to new_deps and removed_deps.
        """
        # Create an existing build dependency
        existing_dep = LegacyDependency(
            id=1,
            package_id=package_ids["main"],
            dependency_id=package_ids["dep"],
            dependency_type_id=mock_config.dependency_types.build,  # BUILD type
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )

        # Create diff with existing dependency
        diff = diff_instance(
            package_map={"1048221": packages["main"], "271975": packages["dep"]},
            dependencies={package_ids["main"]: {existing_dep}},
        )

        # Create crate with dependency changed to runtime
        dependency = CrateDependency(
            crate_id=1048221,
            dependency_id=271975,
            dependency_type=DependencyType.NORMAL,  # Changed to runtime
            semver_range="^0.26.1",
        )
        crate = crate_with_dependencies(dependencies=[dependency])

        # Execute
        new_deps, removed_deps = diff.diff_deps(crate)

        # Assert
        assert len(new_deps) == 1, "One new dep should be added (new type)"
        assert len(removed_deps) == 1, "One dep should be removed (old type)"

        # Verify new dep is runtime
        new_dep = new_deps[0]
        assert new_dep.package_id == package_ids["main"]
        assert new_dep.dependency_id == package_ids["dep"]
        assert new_dep.dependency_type_id == mock_config.dependency_types.runtime

        # Verify removed dep is build
        removed_dep = removed_deps[0]
        assert removed_dep.package_id == package_ids["main"]
        assert removed_dep.dependency_id == package_ids["dep"]
        assert removed_dep.dependency_type_id == mock_config.dependency_types.build

    def test_new_dependency(
        self, packages, package_ids, diff_instance, crate_with_dependencies, mock_config
    ):
        """
        Test that when a dependency doesn't exist in the cache but appears in the
        crate object, it is added to new_deps.
        """
        # Create diff with no existing dependencies
        diff = diff_instance(
            package_map={"1048221": packages["main"], "271975": packages["dep"]}
        )

        # Create crate with a new dependency
        dependency = CrateDependency(
            crate_id=1048221,
            dependency_id=271975,
            dependency_type=DependencyType.NORMAL,
            semver_range="^0.26.1",
        )
        crate = crate_with_dependencies(dependencies=[dependency])

        # Execute
        new_deps, removed_deps = diff.diff_deps(crate)

        # Assert
        assert len(new_deps) == 1, "One new dep should be added"
        assert len(removed_deps) == 0, "No deps should be removed"

        # Verify new dep
        new_dep = new_deps[0]
        assert new_dep.package_id == package_ids["main"]
        assert new_dep.dependency_id == package_ids["dep"]
        assert new_dep.dependency_type_id == mock_config.dependency_types.runtime

    def test_removed_dependency(
        self, packages, package_ids, diff_instance, crate_with_dependencies, mock_config
    ):
        """
        Test that when a dependency exists in the cache but doesn't appear in the
        crate object, it is added to removed_deps.
        """
        # Create an existing dependency
        existing_dep = LegacyDependency(
            id=1,
            package_id=package_ids["main"],
            dependency_id=package_ids["dep"],
            dependency_type_id=mock_config.dependency_types.runtime,
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )

        # Create diff with existing dependency
        diff = diff_instance(
            package_map={"1048221": packages["main"], "271975": packages["dep"]},
            dependencies={package_ids["main"]: {existing_dep}},
        )

        # Create crate with no dependencies
        crate = crate_with_dependencies(dependencies=[])

        # Execute
        new_deps, removed_deps = diff.diff_deps(crate)

        # Assert
        assert len(new_deps) == 0, "No new deps should be added"
        assert len(removed_deps) == 1, "One dep should be removed"

        # Verify removed dep
        removed_dep = removed_deps[0]
        assert removed_dep.package_id == package_ids["main"]
        assert removed_dep.dependency_id == package_ids["dep"]
        assert removed_dep.dependency_type_id == mock_config.dependency_types.runtime

    def test_multiple_dependency_types_same_package(
        self, packages, package_ids, diff_instance, crate_with_dependencies, mock_config
    ):
        """
        Test that when a package depends on the same dependency package with
        multiple dependency types (e.g., both runtime and build), we handle
        the unique constraint on (package_id, dependency_id) properly.

        This test exposes the bug where multiple LegacyDependency records with
        the same package_id and dependency_id but different dependency_type_id
        would violate the DB constraint.
        """
        # Create diff with no existing dependencies
        diff = diff_instance(
            package_map={"1048221": packages["main"], "271975": packages["dep"]}
        )

        # Create crate with multiple dependency types to the same package
        runtime_dep = CrateDependency(
            crate_id=1048221,
            dependency_id=271975,
            dependency_type=DependencyType.NORMAL,  # Runtime
            semver_range="^0.26.1",
        )
        build_dep = CrateDependency(
            crate_id=1048221,
            dependency_id=271975,
            dependency_type=DependencyType.BUILD,  # Build
            semver_range="^0.26.1",
        )

        crate = crate_with_dependencies(dependencies=[runtime_dep, build_dep])

        # Execute
        new_deps, removed_deps = diff.diff_deps(crate)

        # Assert
        assert len(removed_deps) == 0, "No deps should be removed"

        # With the fix, only create 1 dependency with highest priority type
        # Priority: NORMAL > BUILD > DEV
        assert len(new_deps) == 1, "Should create only 1 dep with highest priority type"

        # Should have runtime type (NORMAL has highest priority)
        new_dep = new_deps[0]
        assert new_dep.package_id == package_ids["main"]
        assert new_dep.dependency_id == package_ids["dep"]
        assert new_dep.dependency_type_id == mock_config.dependency_types.runtime, (
            "Should choose NORMAL (runtime) over BUILD as it has higher priority"
        )

    def test_multiple_dependency_types_build_vs_dev(
        self, packages, package_ids, diff_instance, crate_with_dependencies, mock_config
    ):
        """
        Test that when a package depends on the same dependency package with
        BUILD and DEV types (no NORMAL), BUILD type takes precedence.

        Priority order: NORMAL > BUILD > DEV
        """
        # Create diff with no existing dependencies
        diff = diff_instance(
            package_map={"1048221": packages["main"], "271975": packages["dep"]}
        )

        # Create crate with BUILD and DEV dependencies to the same package
        build_dep = CrateDependency(
            crate_id=1048221,
            dependency_id=271975,
            dependency_type=DependencyType.BUILD,
            semver_range="^0.26.1",
        )
        dev_dep = CrateDependency(
            crate_id=1048221,
            dependency_id=271975,
            dependency_type=DependencyType.DEV,
            semver_range="^0.26.1",
        )

        # Add DEV first to test ordering doesn't matter
        crate = crate_with_dependencies(dependencies=[dev_dep, build_dep])

        # Execute
        new_deps, removed_deps = diff.diff_deps(crate)

        # Assert
        assert len(removed_deps) == 0, "No deps should be removed"
        assert len(new_deps) == 1, "Should create only 1 dep with highest priority type"

        # Should have build type (BUILD > DEV)
        new_dep = new_deps[0]
        assert new_dep.package_id == package_ids["main"]
        assert new_dep.dependency_id == package_ids["dep"]
        assert new_dep.dependency_type_id == mock_config.dependency_types.build, (
            "Should choose BUILD over DEV as it has higher priority"
        )
