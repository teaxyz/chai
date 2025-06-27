"""
Test the diff_deps functionality for the homebrew package manager.

This module tests the Diff.diff_deps method which determines what dependencies
need to be added or removed when processing homebrew formula updates.
"""

from datetime import datetime
from uuid import UUID, uuid4

import pytest

from core.models import LegacyDependency, Package
from package_managers.homebrew.diff import Diff
from package_managers.homebrew.structs import Actual


@pytest.mark.transformer
class TestDiffDeps:
    """Tests for the diff_deps method in the Diff class."""

    def test_new_package_not_in_cache(self, packages, diff_instance, homebrew_formula):
        """
        If the package is not even in the package cache, that means it is new.
        Since we won't know the ID of the package during dependency loading,
        we're going to continue to the next package and write a warning.
        """
        # Create cache without the package we'll look for
        diff = diff_instance(
            package_map={"bar": packages["bar"], "baz": packages["baz"]}
        )

        # Create an Actual package that's not in the cache
        new_pkg = homebrew_formula(
            "new_package", dependencies=["baz"], build_dependencies=["bar"]
        )

        # Execute
        new_deps, removed_deps = diff.diff_deps(new_pkg)

        # Assert
        assert len(new_deps) == 0, "No new deps for new pkg"
        assert len(removed_deps) == 0, "No removed deps for new pkg"

    def test_existing_package_adding_dependency(
        self,
        packages: dict[str, Package],
        package_ids: dict[str, UUID],
        diff_instance: Diff,
        homebrew_formula: Actual,
        mock_config,
    ):
        """Test diff_deps when adding a new dependency to an existing package."""
        # Create existing dependency
        existing_dep = LegacyDependency(
            id=uuid4(),
            package_id=package_ids["foo"],
            dependency_id=package_ids["bar"],
            dependency_type_id=mock_config.dependency_types.runtime,
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )

        # Create diff with existing dependency
        diff: Diff = diff_instance(
            package_map=packages, dependencies={package_ids["foo"]: {existing_dep}}
        )
        for k, s in diff.caches.dependencies.items():
            print(f"Package: {k}")
            for v in s:
                print(v.dependency_id)
                print(v.dependency_type_id)

        # Create formula with existing dependency plus a new one
        pkg = homebrew_formula(
            "foo",
            dependencies=["bar"],  # existing dependency
            build_dependencies=["baz"],  # new dependency
        )

        # Execute
        new_deps, removed_deps = diff.diff_deps(pkg)

        for dep in new_deps:
            print(
                f"{dep.package_id} -> {dep.dependency_id} as {dep.dependency_type_id}"
            )

        # Assert
        assert len(new_deps) == 1, "One new dependency should be added"
        assert len(removed_deps) == 0, "No dependencies should be removed"

        # Verify new dep is a build dep on baz
        new_dep = new_deps[0]
        assert new_dep.package_id == package_ids["foo"]
        assert new_dep.dependency_id == package_ids["baz"]
        assert new_dep.dependency_type_id == mock_config.dependency_types.build

    def test_existing_package_removing_dependency(
        self, packages, package_ids, diff_instance, homebrew_formula, mock_config
    ):
        """Test diff_deps when removing a dependency from an existing package."""
        # Create existing dependencies
        existing_dep = LegacyDependency(
            id=uuid4(),
            package_id=package_ids["foo"],
            dependency_id=package_ids["bar"],
            dependency_type_id=mock_config.dependency_types.runtime,
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )

        to_be_removed_dep = LegacyDependency(
            id=uuid4(),
            package_id=package_ids["foo"],
            dependency_id=package_ids["baz"],
            dependency_type_id=mock_config.dependency_types.build,
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )

        # Create diff with both dependencies
        diff = diff_instance(
            package_map=packages,
            dependencies={package_ids["foo"]: {existing_dep, to_be_removed_dep}},
        )

        # Create formula with only one of the previous dependencies
        pkg = homebrew_formula(
            "foo",
            dependencies=["bar"],  # only keep this dependency
        )

        # Execute
        new_deps, removed_deps = diff.diff_deps(pkg)

        # Assert
        assert len(new_deps) == 0, "No new deps should be added"
        assert len(removed_deps) == 1, "One dep should be removed"

        # Verify removed dep is a build dep on baz
        removed_dep = removed_deps[0]
        assert removed_dep.package_id == package_ids["foo"]
        assert removed_dep.dependency_id == package_ids["baz"]
        assert removed_dep.dependency_type_id == mock_config.dependency_types.build

    def test_existing_package_changing_dependency_type(
        self, packages, package_ids, diff_instance, homebrew_formula, mock_config
    ):
        """
        If the dependency types for a specific package to package relationship change,
        then Diff sees two changes: one removal and one addition.
        """
        # Create existing runtime dependency
        existing_dep = LegacyDependency(
            id=uuid4(),
            package_id=package_ids["foo"],
            dependency_id=package_ids["bar"],
            dependency_type_id=mock_config.dependency_types.runtime,
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )

        # Create diff with existing dependency
        diff = diff_instance(
            package_map={"foo": packages["foo"], "bar": packages["bar"]},
            dependencies={package_ids["foo"]: {existing_dep}},
        )

        # Create formula with same dependency but changed type
        pkg = homebrew_formula(
            "foo",
            build_dependencies=["bar"],  # Changed from runtime to build
        )

        # Execute
        new_deps, removed_deps = diff.diff_deps(pkg)

        # Assert
        assert len(new_deps) == 1, "One new dep should be added (new type)"
        assert len(removed_deps) == 1, "One dep should be removed (old type)"

        # Verify removed dep is runtime
        removed_dep = removed_deps[0]
        assert removed_dep.package_id == package_ids["foo"]
        assert removed_dep.dependency_id == package_ids["bar"]
        assert removed_dep.dependency_type_id == mock_config.dependency_types.runtime

        # Verify new dep is build
        new_dep = new_deps[0]
        assert new_dep.package_id == package_ids["foo"]
        assert new_dep.dependency_id == package_ids["bar"]
        assert new_dep.dependency_type_id == mock_config.dependency_types.build

    def test_existing_package_no_dependency_changes(
        self, packages, package_ids, diff_instance, homebrew_formula, mock_config
    ):
        """
        Test a case where there's no changes to be made, because the database and
        Homebrew's JSON response indicate the same data.
        """
        # Create existing dependency
        existing_dep = LegacyDependency(
            id=uuid4(),
            package_id=package_ids["foo"],
            dependency_id=package_ids["bar"],
            dependency_type_id=mock_config.dependency_types.runtime,
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )

        # Create diff with existing dependency
        diff = diff_instance(
            package_map={"foo": packages["foo"], "bar": packages["bar"]},
            dependencies={package_ids["foo"]: {existing_dep}},
        )

        # Create formula with same dependency and type
        pkg = homebrew_formula(
            "foo",
            dependencies=["bar"],  # same dependency with same type
        )

        # Execute
        new_deps, removed_deps = diff.diff_deps(pkg)

        # Assert
        assert len(new_deps) == 0, "No new deps should be added"
        assert len(removed_deps) == 0, "No deps should be removed"

    def test_existing_package_same_dependency_multiple_times_no_changes(
        self, packages, package_ids, diff_instance, homebrew_formula, mock_config
    ):
        """
        The case here is that the formula specifies a runtime and build dependency,
        and the db already captured the runtime dependency. Since the Diff class has
        a hierarchy of which dependency to choose, and runtime is on top, we should
        see no changes.
        """
        # Create existing runtime dependency
        existing_dep = LegacyDependency(
            id=uuid4(),
            package_id=package_ids["foo"],
            dependency_id=package_ids["bar"],
            dependency_type_id=mock_config.dependency_types.runtime,
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )

        # Create diff with existing dependency
        diff = diff_instance(
            package_map={"foo": packages["foo"], "bar": packages["bar"]},
            dependencies={package_ids["foo"]: {existing_dep}},
        )

        # Create formula with same dependency multiple times
        pkg = homebrew_formula("foo", dependencies=["bar"], build_dependencies=["bar"])

        # Execute
        new_deps, removed_deps = diff.diff_deps(pkg)

        # Assert
        # Since runtime is encountered first and that's in the DB/cache,
        # we should see no new dependencies
        assert len(new_deps) == 0, "No new deps should be added"
        assert len(removed_deps) == 0, "No deps should be removed"

    def test_existing_package_same_dependency_multiple_times_yes_changes(
        self, packages, package_ids, diff_instance, homebrew_formula, mock_config
    ):
        """
        In this case, suppose the DB maintained a build relationship between foo and bar
        and actually there is a runtime and build dependency according to Homebrew. Here
        CHAI updates this record to a runtime dependency.
        """
        # Create existing build dependency
        existing_dep = LegacyDependency(
            id=uuid4(),
            package_id=package_ids["foo"],
            dependency_id=package_ids["bar"],
            dependency_type_id=mock_config.dependency_types.build,
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )

        # Create diff with existing dependency
        diff = diff_instance(
            package_map={"foo": packages["foo"], "bar": packages["bar"]},
            dependencies={package_ids["foo"]: {existing_dep}},
        )

        # Create formula with same dependency multiple times
        pkg = homebrew_formula(
            "foo",
            dependencies=["bar"],  # runtime has higher priority
            build_dependencies=["bar"],
        )

        # Execute
        new_deps, removed_deps = diff.diff_deps(pkg)

        # Assert
        assert len(new_deps) == 1, "One new dependency should be added"
        assert new_deps[0].dependency_type_id == mock_config.dependency_types.runtime, (
            "The new dependency should be runtime"
        )

        assert len(removed_deps) == 1, "The build dependency should be removed"
        assert (
            removed_deps[0].dependency_type_id == mock_config.dependency_types.build
        ), "The removed dependency should be build"
