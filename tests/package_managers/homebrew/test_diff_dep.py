import unittest
from datetime import datetime
from unittest.mock import MagicMock
from uuid import uuid4

from core.config import Config, DependencyTypes
from core.models import LegacyDependency, Package
from package_managers.homebrew.diff import Diff
from package_managers.homebrew.structs import Actual, Cache


class TestDiffDeps(unittest.TestCase):
    """Tests for the diff_deps method in the Diff class."""

    def setUp(self):
        """Set up common test data and mocks."""
        # Create fixed UUIDs for dependency types for consistent testing
        self.runtime_type_id = uuid4()
        self.build_type_id = uuid4()
        self.test_type_id = uuid4()
        self.recommended_type_id = uuid4()
        self.optional_type_id = uuid4()

        # Mock dependency types with predefined UUIDs
        self.mock_dep_types = MagicMock(spec=DependencyTypes)
        self.mock_dep_types.runtime = self.runtime_type_id
        self.mock_dep_types.build = self.build_type_id
        self.mock_dep_types.test = self.test_type_id
        self.mock_dep_types.recommended = self.recommended_type_id
        self.mock_dep_types.optional = self.optional_type_id

        # Create a mock Config that returns our mock dependency types
        self.mock_config = MagicMock(spec=Config)
        self.mock_config.dependency_types = self.mock_dep_types

        # Common package IDs for testing
        self.foo_id = uuid4()
        self.bar_id = uuid4()
        self.baz_id = uuid4()
        self.qux_id = uuid4()

        # Common package objects
        self.foo_pkg = Package(
            id=self.foo_id,
            name="foo",
            package_manager_id=1,
            import_id="foo",
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )

        self.bar_pkg = Package(
            id=self.bar_id,
            name="bar",
            package_manager_id=1,
            import_id="bar",
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )

        self.baz_pkg = Package(
            id=self.baz_id,
            name="baz",
            package_manager_id=1,
            import_id="baz",
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )

        self.qux_pkg = Package(
            id=self.qux_id,
            name="qux",
            package_manager_id=1,
            import_id="qux",
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )

    def test_new_package_not_in_cache(self):
        """If the package is not even in the package cache, that means it is new. Since,
        we won't know the ID of the package during dependency loading, we're going to
        continue to the next package, and write a warning statement, since the DB
        will eventually converge"""
        # arrange
        # create a cache without the package we'll look for
        cache = Cache(
            package_cache={
                "bar": self.bar_pkg,
                "baz": self.baz_pkg,
            },
            url_cache={},
            package_url_cache={},
            dependency_cache={},
        )

        # create Diff instance with our mock config and cache
        diff = Diff(self.mock_config, cache)

        # create an Actual package that's not in the cache
        new_pkg = Actual(
            formula="new_package",
            description="A new package",
            license="MIT",
            homepage="",
            source="",
            repository="",
            build_dependencies=["bar"],
            dependencies=["baz"],
            test_dependencies=[],
            recommended_dependencies=[],
            optional_dependencies=[],
        )

        # act
        new_deps, removed_deps = diff.diff_deps(new_pkg)

        # assert
        self.assertEqual(len(new_deps), 0, "No new deps for new pkg")
        self.assertEqual(len(removed_deps), 0, "No removed deps for new pkg")

    def test_existing_package_adding_dependency(self):
        """
        Test diff_deps when adding a new dependency to an existing package.
        """
        # arrange
        # create cache with existing package and some dependencies
        cache = Cache(
            package_cache={
                "foo": self.foo_pkg,
                "bar": self.bar_pkg,
                "baz": self.baz_pkg,
                "qux": self.qux_pkg,
            },
            url_cache={},
            package_url_cache={},
            dependency_cache={
                self.foo_id: {
                    LegacyDependency(
                        id=uuid4(),
                        package_id=self.foo_id,
                        dependency_id=self.bar_id,
                        dependency_type_id=self.runtime_type_id,
                        created_at=datetime.now(),
                        updated_at=datetime.now(),
                    )
                }
            },
        )

        diff = Diff(self.mock_config, cache)

        # create an Actual package with the existing dependency plus a new one
        pkg = Actual(
            formula="foo",
            description="",
            license="",
            homepage="",
            source="",
            repository="",
            build_dependencies=["baz"],  # new dependency
            dependencies=["bar"],  # existing dependency
            test_dependencies=[],
            recommended_dependencies=[],
            optional_dependencies=[],
        )

        # act
        new_deps, removed_deps = diff.diff_deps(pkg)

        # assert
        self.assertEqual(len(new_deps), 1, "One new dependency should be added")
        self.assertEqual(len(removed_deps), 0, "No dependencies should be removed")

        # new dep should be a build dep on baz
        new_dep = new_deps[0]
        self.assertEqual(new_dep.package_id, self.foo_id)
        self.assertEqual(new_dep.dependency_id, self.baz_id)
        self.assertEqual(new_dep.dependency_type_id, self.build_type_id)

    def test_existing_package_removing_dependency(self):
        """
        Test diff_deps when removing a dependency from an existing package.
        """
        # arrange
        # create an existing dependency
        existing_dep = LegacyDependency(
            id=uuid4(),
            package_id=self.foo_id,
            dependency_id=self.bar_id,
            dependency_type_id=self.runtime_type_id,
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )

        # create a second existing dependency that will be removed
        to_be_removed_dep = LegacyDependency(
            id=uuid4(),
            package_id=self.foo_id,
            dependency_id=self.baz_id,
            dependency_type_id=self.build_type_id,
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )

        # create a cache with existing package and dependencies
        cache = Cache(
            package_cache={
                "foo": self.foo_pkg,
                "bar": self.bar_pkg,
                "baz": self.baz_pkg,
            },
            url_cache={},
            package_url_cache={},
            dependency_cache={self.foo_id: {existing_dep, to_be_removed_dep}},
        )

        diff = Diff(self.mock_config, cache)

        # create an Actual package with only one of the previous dependencies
        pkg = Actual(
            formula="foo",
            description="",
            license="",
            homepage="",
            source="",
            repository="",
            build_dependencies=[],
            dependencies=["bar"],  # only keep this dependency
            test_dependencies=[],
            recommended_dependencies=[],
            optional_dependencies=[],
        )

        # act
        new_deps, removed_deps = diff.diff_deps(pkg)

        # assert
        self.assertEqual(len(new_deps), 0, "No new deps should be added")
        self.assertEqual(len(removed_deps), 1, "One dep should be removed")

        # removed dep should be a build dep on baz
        removed_dep = removed_deps[0]
        self.assertEqual(removed_dep.package_id, self.foo_id)
        self.assertEqual(removed_dep.dependency_id, self.baz_id)
        self.assertEqual(removed_dep.dependency_type_id, self.build_type_id)

    def test_existing_package_changing_dependency_type(self):
        """
        If the dependency types for a specific package to package relationship, then
        Diff sees two changes:
          - One removal
          - One addition

          Note that the removal happens first
        """
        # arrange
        # create an existing dependency with runtime type
        existing_dep = LegacyDependency(
            id=uuid4(),
            package_id=self.foo_id,
            dependency_id=self.bar_id,
            dependency_type_id=self.runtime_type_id,
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )

        # create a cache with existing package and dependency
        cache = Cache(
            package_cache={
                "foo": self.foo_pkg,
                "bar": self.bar_pkg,
            },
            url_cache={},
            package_url_cache={},
            dependency_cache={self.foo_id: {existing_dep}},
        )

        diff = Diff(self.mock_config, cache)

        # create an Actual package with the same dependency but changed type
        pkg = Actual(
            formula="foo",
            description="",
            license="",
            homepage="",
            source="",
            repository="",
            build_dependencies=["bar"],  # Changed from runtime to build
            dependencies=[],
            test_dependencies=[],
            recommended_dependencies=[],
            optional_dependencies=[],
        )

        # act
        new_deps, removed_deps = diff.diff_deps(pkg)

        # assert
        self.assertEqual(len(new_deps), 1, "One new dep should be added (new type)")
        self.assertEqual(len(removed_deps), 1, "One dep should be removed (old type)")

        # removed dep should be a runtime dep on bar
        removed_dep = removed_deps[0]
        self.assertEqual(removed_dep.package_id, self.foo_id)
        self.assertEqual(removed_dep.dependency_id, self.bar_id)
        self.assertEqual(removed_dep.dependency_type_id, self.runtime_type_id)

        # new dep should be a build dep on bar
        new_dep = new_deps[0]
        self.assertEqual(new_dep.package_id, self.foo_id)
        self.assertEqual(new_dep.dependency_id, self.bar_id)
        self.assertEqual(new_dep.dependency_type_id, self.build_type_id)

    def test_existing_package_no_dependency_changes(self):
        """
        Test a case where there's no changes to be made, because the database and
        Homebrew's JSON response indciate the same data
        """
        # arrange
        # create an existing dependency
        existing_dep = LegacyDependency(
            id=uuid4(),
            package_id=self.foo_id,
            dependency_id=self.bar_id,
            dependency_type_id=self.runtime_type_id,
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )

        # create a cache with existing package and dependency
        cache = Cache(
            package_cache={
                "foo": self.foo_pkg,
                "bar": self.bar_pkg,
            },
            url_cache={},
            package_url_cache={},
            dependency_cache={self.foo_id: {existing_dep}},
        )

        diff = Diff(self.mock_config, cache)

        # create an Actual package with the same dependency and type
        pkg = Actual(
            formula="foo",
            description="",
            license="",
            homepage="",
            source="",
            repository="",
            build_dependencies=[],
            dependencies=["bar"],  # same dependency with same type
            test_dependencies=[],
            recommended_dependencies=[],
            optional_dependencies=[],
        )

        # act
        new_deps, removed_deps = diff.diff_deps(pkg)

        # assert
        self.assertEqual(len(new_deps), 0, "No new deps should be added")
        self.assertEqual(len(removed_deps), 0, "No deps should be removed")

    def test_existing_package_same_dependency_multiple_times_no_changes(self):
        """
        The case here is that the formula specifies a runtime and build dependency,
        and the db already captured the runtime dependency. Since the Diff class has
        a hierarchy of which dependency to choose, and runtime is on top, we should
        see no changes.
        """
        # arrange
        # create an existing dependency
        existing_dep = LegacyDependency(
            id=uuid4(),
            package_id=self.foo_id,
            dependency_id=self.bar_id,
            dependency_type_id=self.runtime_type_id,
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )

        # create a cache with existing package and dependency
        cache = Cache(
            package_cache={
                "foo": self.foo_pkg,
                "bar": self.bar_pkg,
            },
            url_cache={},
            package_url_cache={},
            dependency_cache={self.foo_id: {existing_dep}},
        )

        diff = Diff(self.mock_config, cache)

        # create an Actual package with the same dependency multiple times
        pkg = Actual(
            formula="foo",
            description="",
            license="",
            homepage="",
            source="",
            repository="",
            build_dependencies=["bar"],
            dependencies=["bar"],
            test_dependencies=[],
            recommended_dependencies=[],
            optional_dependencies=[],
        )

        # act
        new_deps, removed_deps = diff.diff_deps(pkg)

        # assert
        # since the runtime is encountered first, and that's in the DB / cache.
        # then we should see no new depencides
        self.assertEqual(len(new_deps), 0, "No new deps should be added")

        # nothing to be removed either
        self.assertEqual(len(removed_deps), 0, "No deps should be removed")

    def test_existing_package_same_dependency_multiple_times_yes_changes(self):
        """
        In this case, suppose the DB maintained a build relationship between foo and bar
        and actually there is a runtime and build dependency according to Homebrew. Here
        CHAI updates this record to a runtime dependency
        """
        # arrange
        # create an existing dependency with runtime type
        existing_dep = LegacyDependency(
            id=uuid4(),
            package_id=self.foo_id,
            dependency_id=self.bar_id,
            dependency_type_id=self.build_type_id,
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )

        # create cache with existing package and dependency
        cache = Cache(
            package_cache={
                "foo": self.foo_pkg,
                "bar": self.bar_pkg,
            },
            url_cache={},
            package_url_cache={},
            dependency_cache={self.foo_id: {existing_dep}},
        )

        diff = Diff(self.mock_config, cache)

        # create an Actual package with the same dependency multiple times
        pkg = Actual(
            formula="foo",
            description="",
            license="",
            homepage="",
            source="",
            repository="",
            build_dependencies=["bar"],
            dependencies=["bar"],
            test_dependencies=[],
            recommended_dependencies=[],
            optional_dependencies=[],
        )

        # act
        new_deps, removed_deps = diff.diff_deps(pkg)

        # assert
        # since the build is encountered first, and that's in the DB / cache.
        # then we should see one new dependency
        self.assertEqual(len(new_deps), 1, "One new dependency should be added")

        # the dependency should be the runtime dependency
        self.assertEqual(new_deps[0].dependency_type_id, self.runtime_type_id)

        # nothing to be removed either
        self.assertEqual(len(removed_deps), 1, "The build dependency should be removed")

        # the removed dependency should be the build dependency
        self.assertEqual(removed_deps[0].dependency_type_id, self.build_type_id)
