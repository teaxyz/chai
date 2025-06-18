#!/usr/bin/env pkgx uv run

from unittest.mock import patch
from uuid import uuid4

from core.models import URL, LegacyDependency, Package, PackageURL
from core.structs import Cache, URLKey
from package_managers.pkgx.diff import PkgxDiff
from package_managers.pkgx.parser import (
    Dependency,
    DependencyBlock,
    Distributable,
    PkgxPackage,
    Version,
)


def create_pkgx_package(
    distributables: list[str] | None = None,
    dependencies: list[str] | None = None,
    build_deps: list[str] | None = None,
    test_deps: list[str] | None = None,
) -> PkgxPackage:
    """Helper to create PkgxPackage instances for testing"""

    # Create distributable blocks
    distributable_blocks = []
    if distributables:
        for url in distributables:
            distributable_blocks.append(Distributable(url=url))

    # Create dependency objects
    dep_objects = [
        DependencyBlock(
            platform="all",
            dependencies=[
                Dependency(name=dep, semver="*") for dep in (dependencies or [])
            ],
        )
    ]
    build_dep_objects = [
        DependencyBlock(
            platform="all",
            dependencies=[
                Dependency(name=dep, semver="*") for dep in (build_deps or [])
            ],
        )
    ]
    test_dep_objects = [
        DependencyBlock(
            platform="all",
            dependencies=[
                Dependency(name=dep, semver="*") for dep in (test_deps or [])
            ],
        )
    ]

    # Create version object
    version = Version()

    return PkgxPackage(
        distributable=distributable_blocks,
        versions=version,
        dependencies=dep_objects,
        build=DependencyBlock(platform="linux", dependencies=build_dep_objects),
        test=DependencyBlock(platform="linux", dependencies=test_dep_objects),
    )


class TestPkgxDifferentialLoading:
    """Test cases for pkgx differential loading scenarios"""

    def test_package_exists_url_update(self, mock_config, mock_logger, mock_db):
        """Test scenario 2: Package existed in database and needed a URL update"""

        # Setup existing package and URL
        existing_pkg_id = uuid4()
        existing_url_id = uuid4()
        existing_package_url_id = uuid4()

        existing_package = Package(
            id=existing_pkg_id,
            derived_id="pkgx/url-pkg",
            name="url-pkg",
            package_manager_id=mock_config.pm_config.pm_id,
            import_id="url-pkg",
            readme="Test package",
        )

        existing_url = URL(
            id=existing_url_id,
            url="https://old-source.com/file.tar.gz",
            url_type_id=mock_config.url_types.source,
        )

        existing_package_url = PackageURL(
            id=existing_package_url_id,
            package_id=existing_pkg_id,
            url_id=existing_url_id,
        )

        # Create cache
        cache = Cache(
            package_map={"url-pkg": existing_package},
            url_map={
                URLKey(
                    "https://old-source.com/file.tar.gz", mock_config.url_types.source
                ): existing_url
            },
            package_urls={existing_pkg_id: {existing_package_url}},
            dependencies={},
        )

        # Create package data with new URL
        new_pkg_data = create_pkgx_package(
            distributables=["https://new-source.com/file.tar.gz"],
        )
        new_generated_urls = [
            URLKey("https://new-source.com/file.tar.gz", mock_config.url_types.source)
        ]
        new_urls = {}  # this tracks all the new URLs we've created so far -
        # let it be empty for this test

        # Test the diff
        diff = PkgxDiff(mock_config, cache, mock_db, mock_logger)

        # Mock the URL retrieval step
        with (
            patch(
                "package_managers.pkgx.diff.generate_chai_urls",
                return_value=new_generated_urls,
            ),
        ):
            resolved_urls = diff.diff_url("url-pkg", new_pkg_data, new_urls)
            new_links, _ = diff.diff_pkg_url(existing_pkg_id, resolved_urls)

        # Assertions
        assert len(new_links) == 1  # New URL should be created
        new_link = new_links[0]
        assert new_link.package_id == existing_pkg_id

        # The URL should be created in new_urls dict and the link should reference it
        assert len(new_urls) == 1  # One new URL should be created
        new_url_key = list(new_urls.keys())[0]
        new_url = new_urls[new_url_key]
        assert new_link.url_id == new_url.id  # Link should reference the new URL
        assert new_url_key.url == "https://new-source.com/file.tar.gz"
        assert new_url_key.url_type_id == mock_config.url_types.source

    def test_package_exists_dependency_change(self, mock_config, mock_logger, mock_db):
        """Test scenario 3: Package existed in database and changed its dependencies"""

        # Setup existing package and dependencies
        existing_pkg_id = uuid4()
        dep1_id = uuid4()
        dep2_id = uuid4()
        dep3_id = uuid4()

        existing_package = Package(
            id=existing_pkg_id,
            derived_id="pkgx/dep-pkg",
            name="dep-pkg",
            package_manager_id=mock_config.pm_config.pm_id,
            import_id="dep-pkg",
            readme="",
        )

        # Create dependency packages
        dep1_pkg = Package(
            id=dep1_id, derived_id="pkgx/dep1", name="dep1", import_id="dep1"
        )
        dep2_pkg = Package(
            id=dep2_id, derived_id="pkgx/dep2", name="dep2", import_id="dep2"
        )
        dep3_pkg = Package(
            id=dep3_id, derived_id="pkgx/dep3", name="dep3", import_id="dep3"
        )

        # Create existing dependencies (dep1 as runtime, dep2 as build)
        existing_dep1 = LegacyDependency(
            package_id=existing_pkg_id,
            dependency_id=dep1_id,
            dependency_type_id=mock_config.dependency_types.runtime,
        )
        existing_dep2 = LegacyDependency(
            package_id=existing_pkg_id,
            dependency_id=dep2_id,
            dependency_type_id=mock_config.dependency_types.build,
        )

        # Create cache
        cache = Cache(
            package_map={
                "dep-pkg": existing_package,
                "dep1": dep1_pkg,
                "dep2": dep2_pkg,
                "dep3": dep3_pkg,
            },
            url_map={},
            package_urls={},
            dependencies={existing_pkg_id: {existing_dep1, existing_dep2}},
        )

        # Create new package data with changed dependencies
        # Remove dep2, keep dep1, add dep3 as runtime
        new_pkg_data = create_pkgx_package(
            dependencies=["dep1", "dep3"],  # runtime deps
            build_deps=[],  # no build deps (removes dep2)
        )

        # Test the diff
        diff = PkgxDiff(mock_config, cache, mock_db, mock_logger)
        new_deps, removed_deps = diff.diff_deps("dep-pkg", new_pkg_data)

        # Assertions
        assert len(new_deps) == 1  # dep3 should be added
        assert new_deps[0].dependency_id == dep3_id
        assert new_deps[0].dependency_type_id == mock_config.dependency_types.runtime

        assert len(removed_deps) == 1  # dep2 should be removed
        assert removed_deps[0].dependency_id == dep2_id
        assert removed_deps[0].dependency_type_id == mock_config.dependency_types.build

    def test_completely_new_package(self, mock_config, mock_logger, mock_db):
        """Test scenario 4: Package was completely new to the database"""

        # Create empty cache (no existing packages)
        cache = Cache(package_map={}, url_map={}, package_urls={}, dependencies={})

        # Create new package data
        new_pkg_data = create_pkgx_package(
            distributables=["https://github.com/example/new-pkg/archive/v1.0.tar.gz"],
            dependencies=["some-dep"],
            build_deps=["build-tool"],
        )

        # Test the diff
        diff = PkgxDiff(mock_config, cache, mock_db, mock_logger)
        pkg_id, pkg_obj, update_payload = diff.diff_pkg("new-pkg", new_pkg_data)

        # Assertions
        assert pkg_obj is not None  # New package should be created
        assert pkg_obj.derived_id == "pkgx/new-pkg"
        assert pkg_obj.name == "new-pkg"
        assert pkg_obj.import_id == "new-pkg"
        assert pkg_obj.package_manager_id == mock_config.pm_config.pm_id
        assert update_payload == {}  # No updates for new package

        # Test URL creation
        new_urls = {}
        # Mock generate_chai_urls to return predictable URLs
        mock_urls = [
            URLKey(
                "https://github.com/example/new-pkg", mock_config.url_types.homepage
            ),
            URLKey(
                "https://github.com/example/new-pkg/archive/v1.0.tar.gz",
                mock_config.url_types.source,
            ),
        ]
        with patch(
            "package_managers.pkgx.diff.generate_chai_urls", return_value=mock_urls
        ):
            resolved_urls = diff.diff_url("new-pkg", new_pkg_data, new_urls)
            new_links, updated_links = diff.diff_pkg_url(pkg_id, resolved_urls)

        # Should create URLs for homepage, source, and repository (GitHub)
        assert len(new_urls) >= 2  # At least source and homepage
        assert len(new_links) >= 2  # At least source and homepage links
        assert len(updated_links) == 0  # No existing links to update

    def test_no_changes_scenario(self, mock_config, mock_logger, mock_db):
        """Test scenario where package exists but has no changes"""

        # Setup existing package
        existing_pkg_id = uuid4()
        existing_package = Package(
            id=existing_pkg_id,
            derived_id="pkgx/unchanged-pkg",
            name="unchanged-pkg",
            package_manager_id=mock_config.pm_config.pm_id,
            import_id="unchanged-pkg",
            readme="Unchanged description",
        )

        cache = Cache(
            package_map={"unchanged-pkg": existing_package},
            url_map={},
            package_urls={},
            dependencies={},
        )

        # Create package data with same description
        pkg_data = create_pkgx_package()

        # Test the diff
        diff = PkgxDiff(mock_config, cache, mock_db, mock_logger)
        pkg_id, pkg_obj, update_payload = diff.diff_pkg("unchanged-pkg", pkg_data)

        # Assertions
        assert pkg_id == existing_pkg_id
        assert pkg_obj is None  # No new package
        assert update_payload is None  # No changes

    def test_missing_dependency_handling(self, mock_config, mock_logger, mock_db):
        """Test how missing dependencies are handled"""

        existing_pkg_id = uuid4()
        existing_package = Package(
            id=existing_pkg_id,
            derived_id="pkgx/missing-dep-pkg",
            name="missing-dep-pkg",
            import_id="missing-dep-pkg",
        )

        cache = Cache(
            package_map={"missing-dep-pkg": existing_package},
            url_map={},
            package_urls={},
            dependencies={},
        )

        # Create package with dependency that doesn't exist in cache
        pkg_data = create_pkgx_package(dependencies=["non-existent-dep"])

        diff = PkgxDiff(mock_config, cache, mock_db, mock_logger)
        new_deps, removed_deps = diff.diff_deps("missing-dep-pkg", pkg_data)

        # Should handle gracefully - no deps added for missing packages
        assert len(new_deps) == 0
        assert len(removed_deps) == 0

    def test_dependency_type_priority_no_change(
        self, mock_config, mock_logger, mock_db
    ):
        """Test case 1: p1 has runtime dependency to p2 in cache,
        p1 depends on p2 as both runtime and build in parsed data.
        Expect no change (runtime has priority)."""

        # Setup existing package and dependencies
        p1_id = uuid4()
        p2_id = uuid4()

        p1_pkg = Package(id=p1_id, derived_id="pkgx/p1", name="p1", import_id="p1")
        p2_pkg = Package(id=p2_id, derived_id="pkgx/p2", name="p2", import_id="p2")

        # Existing runtime dependency in cache
        existing_runtime_dep = LegacyDependency(
            package_id=p1_id,
            dependency_id=p2_id,
            dependency_type_id=mock_config.dependency_types.runtime,
        )

        cache = Cache(
            package_map={"p1": p1_pkg, "p2": p2_pkg},
            url_map={},
            package_urls={},
            dependencies={p1_id: {existing_runtime_dep}},
        )

        # Parsed data has p2 as both runtime and build dependency
        new_pkg_data = create_pkgx_package(
            dependencies=["p2"],  # runtime
            build_deps=["p2"],  # build
        )

        diff = PkgxDiff(mock_config, cache, mock_db, mock_logger)
        new_deps, removed_deps = diff.diff_deps("p1", new_pkg_data)

        # Should have no changes - runtime priority means no change needed
        assert len(new_deps) == 0
        assert len(removed_deps) == 0

    def test_dependency_type_change_runtime_to_build(
        self, mock_config, mock_logger, mock_db
    ):
        """Test case 2: p1 has runtime dependency to p2 in cache,
        p1 has build dependency to p2 in parsed data.
        Expect removed runtime dependency and new build dependency."""

        p1_id = uuid4()
        p2_id = uuid4()

        p1_pkg = Package(id=p1_id, derived_id="pkgx/p1", name="p1", import_id="p1")
        p2_pkg = Package(id=p2_id, derived_id="pkgx/p2", name="p2", import_id="p2")

        # Existing runtime dependency
        existing_runtime_dep = LegacyDependency(
            package_id=p1_id,
            dependency_id=p2_id,
            dependency_type_id=mock_config.dependency_types.runtime,
        )

        cache = Cache(
            package_map={"p1": p1_pkg, "p2": p2_pkg},
            url_map={},
            package_urls={},
            dependencies={p1_id: {existing_runtime_dep}},
        )

        # Parsed data only has build dependency
        new_pkg_data = create_pkgx_package(
            dependencies=[],  # no runtime deps
            build_deps=["p2"],  # only build
        )

        diff = PkgxDiff(mock_config, cache, mock_db, mock_logger)
        new_deps, removed_deps = diff.diff_deps("p1", new_pkg_data)

        # Should remove runtime and add build
        assert len(removed_deps) == 1
        assert removed_deps[0].dependency_id == p2_id
        assert (
            removed_deps[0].dependency_type_id == mock_config.dependency_types.runtime
        )

        assert len(new_deps) == 1
        assert new_deps[0].dependency_id == p2_id
        assert new_deps[0].dependency_type_id == mock_config.dependency_types.build

    def test_dependency_type_change_build_to_runtime(
        self, mock_config, mock_logger, mock_db
    ):
        """Test case 3: p1 has build dependency to p2 in cache,
        p1 has runtime dependency to p2 in parsed data.
        Expect removed build dependency and new runtime dependency."""

        p1_id = uuid4()
        p2_id = uuid4()

        p1_pkg = Package(id=p1_id, derived_id="pkgx/p1", name="p1", import_id="p1")
        p2_pkg = Package(id=p2_id, derived_id="pkgx/p2", name="p2", import_id="p2")

        # Existing build dependency
        existing_build_dep = LegacyDependency(
            package_id=p1_id,
            dependency_id=p2_id,
            dependency_type_id=mock_config.dependency_types.build,
        )

        cache = Cache(
            package_map={"p1": p1_pkg, "p2": p2_pkg},
            url_map={},
            package_urls={},
            dependencies={p1_id: {existing_build_dep}},
        )

        # Parsed data only has runtime dependency
        new_pkg_data = create_pkgx_package(
            dependencies=["p2"],  # runtime
            build_deps=[],  # no build deps
        )

        diff = PkgxDiff(mock_config, cache, mock_db, mock_logger)
        new_deps, removed_deps = diff.diff_deps("p1", new_pkg_data)

        # Should remove build and add runtime
        assert len(removed_deps) == 1
        assert removed_deps[0].dependency_id == p2_id
        assert removed_deps[0].dependency_type_id == mock_config.dependency_types.build

        assert len(new_deps) == 1
        assert new_deps[0].dependency_id == p2_id
        assert new_deps[0].dependency_type_id == mock_config.dependency_types.runtime

    def test_dependency_type_priority_new_package(
        self, mock_config, mock_logger, mock_db
    ):
        """Test case 4: p1 has no dependencies to p2 in cache,
        p1 has both runtime and build dependencies to p2 in parsed data.
        Expect one new runtime dependency (priority over build)."""

        p1_id = uuid4()
        p2_id = uuid4()

        p1_pkg = Package(id=p1_id, derived_id="pkgx/p1", name="p1", import_id="p1")
        p2_pkg = Package(id=p2_id, derived_id="pkgx/p2", name="p2", import_id="p2")

        cache = Cache(
            package_map={"p1": p1_pkg, "p2": p2_pkg},
            url_map={},
            package_urls={},
            dependencies={},  # No existing dependencies
        )

        # Parsed data has both runtime and build dependencies to p2
        new_pkg_data = create_pkgx_package(
            dependencies=["p2"],  # runtime
            build_deps=["p2"],  # build
        )

        diff = PkgxDiff(mock_config, cache, mock_db, mock_logger)
        new_deps, removed_deps = diff.diff_deps("p1", new_pkg_data)

        # Should only create one new dependency - runtime (higher priority)
        assert len(removed_deps) == 0
        assert len(new_deps) == 1
        assert new_deps[0].dependency_id == p2_id
        assert new_deps[0].dependency_type_id == mock_config.dependency_types.runtime

    def test_dependency_type_priority_with_test(
        self, mock_config, mock_logger, mock_db
    ):
        """Test priority handling with test dependencies: Runtime > Build > Test"""

        p1_id = uuid4()
        p2_id = uuid4()
        p3_id = uuid4()
        p4_id = uuid4()

        p1_pkg = Package(id=p1_id, derived_id="pkgx/p1", name="p1", import_id="p1")
        p2_pkg = Package(id=p2_id, derived_id="pkgx/p2", name="p2", import_id="p2")
        p3_pkg = Package(id=p3_id, derived_id="pkgx/p3", name="p3", import_id="p3")
        p4_pkg = Package(id=p4_id, derived_id="pkgx/p4", name="p4", import_id="p4")

        cache = Cache(
            package_map={"p1": p1_pkg, "p2": p2_pkg, "p3": p3_pkg, "p4": p4_pkg},
            url_map={},
            package_urls={},
            dependencies={},
        )

        # Parsed data with overlapping dependencies across different types
        new_pkg_data = create_pkgx_package(
            dependencies=["p2", "p3"],  # runtime: p2, p3
            build_deps=["p2", "p4"],  # build: p2, p4
            test_deps=["p2", "p3", "p4"],  # test: p2, p3, p4
        )

        diff = PkgxDiff(mock_config, cache, mock_db, mock_logger)
        new_deps, removed_deps = diff.diff_deps("p1", new_pkg_data)

        # Should create dependencies based on priority:
        # p2: runtime (highest priority among runtime/build/test)
        # p3: runtime (highest priority among runtime/test)
        # p4: build (highest priority among build/test)
        assert len(removed_deps) == 0
        assert len(new_deps) == 3

        # Sort by dependency_id for consistent testing
        new_deps_sorted = sorted(new_deps, key=lambda d: str(d.dependency_id))

        # p2 should be runtime (highest priority)
        p2_dep = next(d for d in new_deps_sorted if d.dependency_id == p2_id)
        assert p2_dep.dependency_type_id == mock_config.dependency_types.runtime

        # p3 should be runtime (highest priority)
        p3_dep = next(d for d in new_deps_sorted if d.dependency_id == p3_id)
        assert p3_dep.dependency_type_id == mock_config.dependency_types.runtime

        # p4 should be build (highest available priority)
        p4_dep = next(d for d in new_deps_sorted if d.dependency_id == p4_id)
        assert p4_dep.dependency_type_id == mock_config.dependency_types.build
