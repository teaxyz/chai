from uuid import uuid4

from core.models import URL, LegacyDependency, Package, PackageURL
from core.structs import Cache, URLKey
from package_managers.debian.diff import DebianDiff
from package_managers.debian.main import diff as main_diff
from tests.package_managers.debian.conftest import create_debian_package


class TestDebianDifferentialLoading:
    """Test cases for debian differential loading scenarios"""

    def test_package_exists_url_update(self, mock_config, mock_logger, mock_db):
        """Tests that Diff updates URLs when the package exists and the URL changes"""

        # Setup existing package and URL
        existing_pkg_id = uuid4()
        existing_url_id = uuid4()
        existing_package_url_id = uuid4()

        existing_package = Package(
            id=existing_pkg_id,
            derived_id="debian/url-pkg",
            name="url-pkg",
            package_manager_id=mock_config.pm_config.pm_id,
            import_id="url-pkg",
            readme="Test package",
        )

        existing_url = URL(
            id=existing_url_id,
            url="https://old-homepage.com",
            url_type_id=mock_config.url_types.homepage,
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
                    "https://old-homepage.com", mock_config.url_types.homepage
                ): existing_url
            },
            package_urls={existing_pkg_id: {existing_package_url}},
            dependencies={},
        )

        # Create package data with new URL
        new_pkg_data = create_debian_package(
            package="url-pkg",
            homepage="https://new-homepage.com",
        )
        new_urls = {}  # this tracks all the new URLs we've created so far

        # Test the diff
        diff = DebianDiff(mock_config, cache, mock_db, mock_logger)
        resolved_urls = diff.diff_url("url-pkg", new_pkg_data, new_urls)
        new_links, _ = diff.diff_pkg_url(existing_pkg_id, resolved_urls)

        # Assertions
        assert len(new_links) == 1  # New URL should be created
        new_link = new_links[0]
        assert new_link.package_id == existing_pkg_id

        # The URL should be created in new_urls dict and the link should reference it
        assert len(new_urls) == 1  # One new URL should be created
        new_url_key = next(iter(new_urls.keys()))
        new_url = new_urls[new_url_key]
        assert new_link.url_id == new_url.id  # Link should reference the new URL
        assert new_url_key.url == "https://new-homepage.com"
        assert new_url_key.url_type_id == mock_config.url_types.homepage

    def test_package_exists_dependency_change(self, mock_config, mock_logger, mock_db):
        """
        Tests that diff correctly records:

          - New dependency
          - Changes to existing dependencies
          - Removed dependencies
        """

        # Setup existing package and dependencies
        existing_pkg_id = uuid4()
        dep1_id = uuid4()
        dep2_id = uuid4()
        dep3_id = uuid4()

        existing_import_id = "debian/dep-pkg"
        existing_package = Package(
            id=existing_pkg_id,
            derived_id=existing_import_id,
            name="dep-pkg",
            package_manager_id=mock_config.pm_config.pm_id,
            import_id=existing_import_id,
            readme="",
        )

        # Create dependency packages
        dep1_pkg = Package(
            id=dep1_id, derived_id="debian/dep1", name="dep1", import_id="debian/dep1"
        )
        dep2_pkg = Package(
            id=dep2_id, derived_id="debian/dep2", name="dep2", import_id="debian/dep2"
        )
        dep3_pkg = Package(
            id=dep3_id, derived_id="debian/dep3", name="dep3", import_id="debian/dep3"
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
                existing_import_id: existing_package,
                "debian/dep1": dep1_pkg,
                "debian/dep2": dep2_pkg,
                "debian/dep3": dep3_pkg,
            },
            url_map={},
            package_urls={},
            dependencies={existing_pkg_id: {existing_dep1, existing_dep2}},
        )

        # Create new package data with changed dependencies
        # Remove dep2, keep dep1, add dep3 as runtime
        new_pkg_data = create_debian_package(
            package="dep-pkg",
            depends=["dep1", "dep3"],  # runtime deps
            build_depends=[],  # no build deps (removes dep2)
        )

        # Test the diff
        diff = DebianDiff(mock_config, cache, mock_db, mock_logger)
        new_deps, removed_deps = diff.diff_deps(existing_import_id, new_pkg_data)

        # Assertions
        assert len(new_deps) == 1  # dep3 should be added
        assert new_deps[0].dependency_id == dep3_id
        assert new_deps[0].dependency_type_id == mock_config.dependency_types.runtime

        assert len(removed_deps) == 1  # dep2 should be removed
        assert removed_deps[0].dependency_id == dep2_id
        assert removed_deps[0].dependency_type_id == mock_config.dependency_types.build

    def test_completely_new_package(self, mock_config, mock_logger, mock_db):
        """Tests the addition of completely new packages & new URLs"""

        # Create empty cache (no existing packages)
        cache = Cache(package_map={}, url_map={}, package_urls={}, dependencies={})

        # Create new package data
        new_pkg_data = create_debian_package(
            package="new-pkg",
            description="A new package",
            homepage="https://github.com/example/new-pkg",
            depends=["some-dep"],
            build_depends=["build-tool"],
        )

        # Test the diff
        diff = DebianDiff(mock_config, cache, mock_db, mock_logger)
        pkg_id, pkg_obj, update_payload = diff.diff_pkg("debian/new-pkg", new_pkg_data)

        # Assertions
        assert pkg_obj is not None  # New package should be created
        assert pkg_obj.derived_id == "debian/new-pkg"
        assert pkg_obj.name == "new-pkg"
        assert pkg_obj.import_id == "debian/new-pkg"
        assert pkg_obj.package_manager_id == mock_config.pm_config.pm_id
        assert pkg_obj.readme == "A new package"
        assert update_payload == {}  # No updates for new package

        # Test URL creation
        new_urls = {}
        resolved_urls = diff.diff_url("new-pkg", new_pkg_data, new_urls)
        new_links, updated_links = diff.diff_pkg_url(pkg_id, resolved_urls)

        # Should create URL for homepage
        assert len(new_urls) >= 1  # At least homepage
        assert len(new_links) >= 1  # At least homepage link
        assert len(updated_links) == 0  # No existing links to update

        # Check that homepage URL was created
        homepage_url_found = False
        for url_key, _url in new_urls.items():
            if url_key.url_type_id == mock_config.url_types.homepage:
                assert url_key.url == "https://github.com/example/new-pkg"
                homepage_url_found = True
                break
        assert homepage_url_found

    def test_no_changes_scenario(self, mock_config, mock_logger, mock_db):
        """Tests where package exists but has no changes"""

        # Setup existing package
        existing_pkg_id = uuid4()
        existing_package = Package(
            id=existing_pkg_id,
            derived_id="debian/unchanged-pkg",
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
        pkg_data = create_debian_package(
            package="unchanged-pkg", description="Unchanged description"
        )

        # Test the diff
        diff = DebianDiff(mock_config, cache, mock_db, mock_logger)
        pkg_id, pkg_obj, update_payload = diff.diff_pkg("unchanged-pkg", pkg_data)

        # Assertions
        assert pkg_id == existing_pkg_id
        assert pkg_obj is None  # No new package
        assert update_payload is None  # No changes

    def test_package_description_update(self, mock_config, mock_logger, mock_db):
        """Test scenario where package exists but description has changed"""

        # Setup existing package
        existing_pkg_id = uuid4()
        existing_package = Package(
            id=existing_pkg_id,
            derived_id="debian/desc-pkg",
            name="desc-pkg",
            package_manager_id=mock_config.pm_config.pm_id,
            import_id="desc-pkg",
            readme="Old description",
        )

        cache = Cache(
            package_map={"desc-pkg": existing_package},
            url_map={},
            package_urls={},
            dependencies={},
        )

        # Create package data with new description
        pkg_data = create_debian_package(
            package="desc-pkg", description="New description"
        )

        # Test the diff
        diff = DebianDiff(mock_config, cache, mock_db, mock_logger)
        pkg_id, pkg_obj, update_payload = diff.diff_pkg("desc-pkg", pkg_data)

        # Assertions
        assert pkg_id == existing_pkg_id
        assert pkg_obj is None  # No new package
        assert update_payload is not None  # Should have changes
        assert update_payload["id"] == existing_pkg_id
        assert update_payload["readme"] == "New description"

    def test_missing_dependency_handling(self, mock_config, mock_logger, mock_db):
        """Tests the case that we DON'T add dependencies for new packages"""

        existing_pkg_id = uuid4()
        existing_package = Package(
            id=existing_pkg_id,
            derived_id="debian/missing-dep-pkg",
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
        pkg_data = create_debian_package(
            package="missing-dep-pkg", depends=["non-existent-dep"]
        )

        diff = DebianDiff(mock_config, cache, mock_db, mock_logger)
        new_deps, removed_deps = diff.diff_deps("missing-dep-pkg", pkg_data)

        # Should handle gracefully - no deps added for missing packages
        assert len(new_deps) == 0
        assert len(removed_deps) == 0

    def test_dependency_type_priority_no_change(
        self, mock_config, mock_logger, mock_db
    ):
        """
        Scenario:
          - p1 has runtime dependency to p2 in cache
          - p1 depends on p2 as both runtime and build in parsed data

        Expect no change (runtime has priority).
        """

        # Setup existing package and dependencies
        p1_id = uuid4()
        p2_id = uuid4()

        p1_pkg = Package(id=p1_id, derived_id="debian/p1", name="p1", import_id="p1")
        p2_pkg = Package(id=p2_id, derived_id="debian/p2", name="p2", import_id="p2")

        # Existing runtime dependency in cache
        existing_runtime_dep = LegacyDependency(
            package_id=p1_id,
            dependency_id=p2_id,
            dependency_type_id=mock_config.dependency_types.runtime,
        )

        cache = Cache(
            package_map={"debian/p1": p1_pkg, "debian/p2": p2_pkg},
            url_map={},
            package_urls={},
            dependencies={p1_id: {existing_runtime_dep}},
        )

        # Parsed data has p2 as both runtime and build dependency
        new_pkg_data = create_debian_package(
            package="p1",
            depends=["p2"],  # runtime
            build_depends=["p2"],  # build
        )

        diff = DebianDiff(mock_config, cache, mock_db, mock_logger)
        new_deps, removed_deps = diff.diff_deps("debian/p1", new_pkg_data)

        # Should have no changes - runtime priority means no change needed
        assert len(new_deps) == 0
        assert len(removed_deps) == 0

    def test_dependency_type_change_runtime_to_build(
        self, mock_config, mock_logger, mock_db
    ):
        """
        Scenario
          - p1 has runtime dependency to p2 in cache
          - p1 has build dependency to p2 in parsed data.

        Expect removed runtime dependency and new build dependency
        """

        p1_id = uuid4()
        p2_id = uuid4()

        p1_pkg = Package(id=p1_id, derived_id="debian/p1", name="p1", import_id="p1")
        p2_pkg = Package(id=p2_id, derived_id="debian/p2", name="p2", import_id="p2")

        # Existing runtime dependency
        existing_runtime_dep = LegacyDependency(
            package_id=p1_id,
            dependency_id=p2_id,
            dependency_type_id=mock_config.dependency_types.runtime,
        )

        cache = Cache(
            package_map={"debian/p1": p1_pkg, "debian/p2": p2_pkg},
            url_map={},
            package_urls={},
            dependencies={p1_id: {existing_runtime_dep}},
        )

        # Parsed data only has build dependency
        new_pkg_data = create_debian_package(
            package="p1",
            depends=[],  # no runtime deps
            build_depends=["p2"],  # only build
        )

        diff = DebianDiff(mock_config, cache, mock_db, mock_logger)
        new_deps, removed_deps = diff.diff_deps("debian/p1", new_pkg_data)

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
        """
        Scenario:
          - p1 has build dependency to p2 in cache
          - p1 has runtime dependency to p2 in parsed data.

        Expect removed build dependency and new runtime dependency
        """

        p1_id = uuid4()
        p2_id = uuid4()

        p1_pkg = Package(id=p1_id, derived_id="debian/p1", name="p1", import_id="p1")
        p2_pkg = Package(id=p2_id, derived_id="debian/p2", name="p2", import_id="p2")

        # Existing build dependency
        existing_build_dep = LegacyDependency(
            package_id=p1_id,
            dependency_id=p2_id,
            dependency_type_id=mock_config.dependency_types.build,
        )

        cache = Cache(
            package_map={"debian/p1": p1_pkg, "debian/p2": p2_pkg},
            url_map={},
            package_urls={},
            dependencies={p1_id: {existing_build_dep}},
        )

        # Parsed data only has runtime dependency
        new_pkg_data = create_debian_package(
            package="p1",
            depends=["p2"],  # runtime
            build_depends=[],  # no build deps
        )

        diff = DebianDiff(mock_config, cache, mock_db, mock_logger)
        new_deps, removed_deps = diff.diff_deps("debian/p1", new_pkg_data)

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
        """
        Scenario:
          - p1 has no dependencies to p2 in cache
          - p1 has both runtime and build dependencies to p2 in parsed data

        Expect one new runtime dependency (priority over build).
        """

        p1_id = uuid4()
        p2_id = uuid4()

        p1_pkg = Package(id=p1_id, derived_id="debian/p1", name="p1", import_id="p1")
        p2_pkg = Package(id=p2_id, derived_id="debian/p2", name="p2", import_id="p2")

        cache = Cache(
            package_map={"debian/p1": p1_pkg, "debian/p2": p2_pkg},
            url_map={},
            package_urls={},
            dependencies={},  # No existing dependencies
        )

        # Parsed data has both runtime and build dependencies to p2
        new_pkg_data = create_debian_package(
            package="p1",
            depends=["p2"],  # runtime
            build_depends=["p2"],  # build
        )

        diff = DebianDiff(mock_config, cache, mock_db, mock_logger)
        new_deps, removed_deps = diff.diff_deps("debian/p1", new_pkg_data)

        # Should only create one new dependency - runtime (higher priority)
        assert len(removed_deps) == 0
        assert len(new_deps) == 1
        assert new_deps[0].dependency_id == p2_id
        assert new_deps[0].dependency_type_id == mock_config.dependency_types.runtime

    def test_debian_specific_dependencies(self, mock_config, mock_logger, mock_db):
        """Test Debian-specific dependency types: recommends, suggests"""

        p1_id = uuid4()
        p2_id = uuid4()
        p3_id = uuid4()

        p1_pkg = Package(id=p1_id, derived_id="debian/p1", name="p1")
        p2_pkg = Package(id=p2_id, derived_id="debian/p2", name="p2")
        p3_pkg = Package(id=p3_id, derived_id="debian/p3", name="p3")

        cache = Cache(
            package_map={"debian/p1": p1_pkg, "debian/p2": p2_pkg, "debian/p3": p3_pkg},
            url_map={},
            package_urls={},
            dependencies={},
        )

        # Parsed data with recommends and suggests (mapped to runtime)
        new_pkg_data = create_debian_package(
            package="p1",
            recommends=["p2"],
            suggests=["p3"],
        )

        diff = DebianDiff(mock_config, cache, mock_db, mock_logger)
        new_deps, removed_deps = diff.diff_deps("debian/p1", new_pkg_data)

        # Should create runtime dependencies for both recommends and suggests
        assert len(removed_deps) == 0
        assert len(new_deps) == 2

        # Both should be runtime dependencies
        for dep in new_deps:
            assert dep.dependency_type_id == mock_config.dependency_types.runtime
            assert dep.dependency_id in [p2_id, p3_id]


class TestDebianDiffFunction:
    """Test cases for the main.diff function"""

    def test_duplicate_package_paragraphs(self, mock_config, mock_logger, mock_db):
        """Tests the case when the Debian Packages file contains duplicate packages"""
        d1 = Package(id=uuid4(), derived_id="debian/d1", name="d1", import_id="d1")
        d2 = Package(id=uuid4(), derived_id="debian/d2", name="d2", import_id="d2")
        p1 = create_debian_package(
            package="linux-doc", homepage="homepage.org", depends=["d1"]
        )
        p2 = create_debian_package(
            package="linux-doc", homepage="homepage.org", depends=["d2"]
        )
        cache = Cache(
            package_map={"debian/d1": d1, "debian/d2": d2},
            url_map={},
            package_urls={},
            dependencies={},
        )

        data = [p1, p2]

        result = main_diff(data, mock_config, cache, mock_db, mock_logger)

        assert len(result.new_packages) == 1
        assert len(result.new_package_urls) == 1
        assert len(result.new_deps) == 0  # bc we don't load dependencies of new pkgs
