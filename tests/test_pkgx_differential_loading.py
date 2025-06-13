#!/usr/bin/env pkgx uv run

import pytest
from datetime import datetime
from uuid import uuid4, UUID
from dataclasses import dataclass
from unittest.mock import Mock, patch

from core.config import Config, PackageManager, URLTypes, DependencyTypes, PackageManagerConfig
from core.models import Package, URL, PackageURL, LegacyDependency
from core.structs import Cache, URLKey
from package_managers.pkgx.diff import PkgxDiff
from package_managers.pkgx.parser import PkgxPackage, DistributableBlock, DependencyBlock, Dependency


@dataclass
class MockConfig:
    """Mock config for testing"""
    pm_config: PackageManagerConfig
    url_types: URLTypes
    dependency_types: DependencyTypes


@pytest.fixture
def mock_config():
    """Create a mock config for testing"""
    pm_id = uuid4()
    homepage_id = uuid4()
    source_id = uuid4()
    repository_id = uuid4()
    build_id = uuid4()
    test_id = uuid4()
    runtime_id = uuid4()
    
    return MockConfig(
        pm_config=PackageManagerConfig(pm_id=pm_id, source="test"),
        url_types=URLTypes(
            homepage=homepage_id,
            source=source_id,
            repository=repository_id
        ),
        dependency_types=DependencyTypes(
            build=build_id,
            test=test_id,
            runtime=runtime_id
        )
    )


def create_pkgx_package(
    description: str = "Test package",
    distributables: list[str] = None,
    dependencies: list[str] = None,
    build_deps: list[str] = None,
    test_deps: list[str] = None
) -> PkgxPackage:
    """Helper to create PkgxPackage instances for testing"""
    
    # Create distributable blocks
    distributable_blocks = []
    if distributables:
        for url in distributables:
            distributable_blocks.append(DistributableBlock(url=url))
    
    # Create dependency objects
    dep_objects = [Dependency(name=dep, semver="*") for dep in (dependencies or [])]
    build_dep_objects = [Dependency(name=dep, semver="*") for dep in (build_deps or [])]
    test_dep_objects = [Dependency(name=dep, semver="*") for dep in (test_deps or [])]
    
    return PkgxPackage(
        description=description,
        distributable=distributable_blocks,
        dependencies=dep_objects,
        build=DependencyBlock(dependencies=build_dep_objects),
        test=DependencyBlock(dependencies=test_dep_objects)
    )


class TestPkgxDifferentialLoading:
    """Test cases for pkgx differential loading scenarios"""

    def test_scenario_1_package_exists_needs_change(self, mock_config):
        """Test scenario 1: Package existed in database and needs to change (description)"""
        
        # Setup existing package in cache
        existing_pkg_id = uuid4()
        existing_package = Package(
            id=existing_pkg_id,
            derived_id="pkgx/test-pkg",
            name="test-pkg",
            package_manager_id=mock_config.pm_config.pm_id,
            import_id="test-pkg",
            readme="Old description"
        )
        
        # Create cache with existing package
        cache = Cache(
            package_map={"test-pkg": existing_package},
            url_map={},
            package_urls={},
            dependencies={}
        )
        
        # Create new package data with changed description
        new_pkg_data = create_pkgx_package(
            description="New updated description",
            distributables=["https://example.com/source.tar.gz"]
        )
        
        # Test the diff
        diff = PkgxDiff(mock_config, cache)
        pkg_id, pkg_obj, update_payload = diff.diff_pkg("test-pkg", new_pkg_data)
        
        # Assertions
        assert pkg_id == existing_pkg_id
        assert pkg_obj is None  # No new package should be created
        assert update_payload is not None
        assert update_payload["id"] == existing_pkg_id
        assert update_payload["readme"] == "New updated description"
        assert "updated_at" in update_payload

    def test_scenario_2_package_exists_url_update(self, mock_config):
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
            readme="Test package"
        )
        
        existing_url = URL(
            id=existing_url_id,
            url="https://old-source.com/file.tar.gz",
            url_type_id=mock_config.url_types.source
        )
        
        existing_package_url = PackageURL(
            id=existing_package_url_id,
            package_id=existing_pkg_id,
            url_id=existing_url_id
        )
        
        # Create cache
        cache = Cache(
            package_map={"url-pkg": existing_package},
            url_map={URLKey("https://old-source.com/file.tar.gz", mock_config.url_types.source): existing_url},
            package_urls={existing_pkg_id: {existing_package_url}},
            dependencies={}
        )
        
        # Create package data with new URL
        new_pkg_data = create_pkgx_package(
            description="Test package",
            distributables=["https://new-source.com/file.tar.gz"]
        )
        
        # Test the diff
        diff = PkgxDiff(mock_config, cache)
        new_urls = {}
        
        # Mock the URL canonicalization and homepage methods
        with patch.object(diff, '_canonicalize_url', side_effect=lambda x: x), \
             patch.object(diff, '_get_homepage_url', return_value=None), \
             patch.object(diff, '_is_github_url', return_value=False):
            
            resolved_urls = diff.diff_url("url-pkg", new_pkg_data, new_urls)
            new_links, updated_links = diff.diff_pkg_url(existing_pkg_id, resolved_urls)
        
        # Assertions
        assert len(new_urls) == 1  # New URL should be created
        new_url = list(new_urls.values())[0]
        assert new_url.url == "https://new-source.com/file.tar.gz"
        assert new_url.url_type_id == mock_config.url_types.source
        
        assert len(new_links) == 1  # New package URL link should be created
        assert new_links[0].package_id == existing_pkg_id
        assert new_links[0].url_id == new_url.id

    def test_scenario_3_package_exists_dependency_change(self, mock_config):
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
            readme="Test package"
        )
        
        # Create dependency packages
        dep1_pkg = Package(id=dep1_id, derived_id="pkgx/dep1", name="dep1", import_id="dep1")
        dep2_pkg = Package(id=dep2_id, derived_id="pkgx/dep2", name="dep2", import_id="dep2")
        dep3_pkg = Package(id=dep3_id, derived_id="pkgx/dep3", name="dep3", import_id="dep3")
        
        # Create existing dependencies (dep1 as runtime, dep2 as build)
        existing_dep1 = LegacyDependency(
            package_id=existing_pkg_id,
            dependency_id=dep1_id,
            dependency_type_id=mock_config.dependency_types.runtime
        )
        existing_dep2 = LegacyDependency(
            package_id=existing_pkg_id,
            dependency_id=dep2_id,
            dependency_type_id=mock_config.dependency_types.build
        )
        
        # Create cache
        cache = Cache(
            package_map={
                "dep-pkg": existing_package,
                "dep1": dep1_pkg,
                "dep2": dep2_pkg,
                "dep3": dep3_pkg
            },
            url_map={},
            package_urls={},
            dependencies={existing_pkg_id: {existing_dep1, existing_dep2}}
        )
        
        # Create new package data with changed dependencies
        # Remove dep2, keep dep1, add dep3 as runtime
        new_pkg_data = create_pkgx_package(
            description="Test package",
            dependencies=["dep1", "dep3"],  # runtime deps
            build_deps=[],  # no build deps (removes dep2)
        )
        
        # Test the diff
        diff = PkgxDiff(mock_config, cache)
        new_deps, removed_deps = diff.diff_deps("dep-pkg", new_pkg_data)
        
        # Assertions
        assert len(new_deps) == 1  # dep3 should be added
        assert new_deps[0].dependency_id == dep3_id
        assert new_deps[0].dependency_type_id == mock_config.dependency_types.runtime
        
        assert len(removed_deps) == 1  # dep2 should be removed
        assert removed_deps[0].dependency_id == dep2_id
        assert removed_deps[0].dependency_type_id == mock_config.dependency_types.build

    def test_scenario_4_completely_new_package(self, mock_config):
        """Test scenario 4: Package was completely new to the database"""
        
        # Create empty cache (no existing packages)
        cache = Cache(
            package_map={},
            url_map={},
            package_urls={},
            dependencies={}
        )
        
        # Create new package data
        new_pkg_data = create_pkgx_package(
            description="Brand new package",
            distributables=["https://github.com/example/new-pkg/archive/v1.0.tar.gz"],
            dependencies=["some-dep"],
            build_deps=["build-tool"]
        )
        
        # Test the diff
        diff = PkgxDiff(mock_config, cache)
        pkg_id, pkg_obj, update_payload = diff.diff_pkg("new-pkg", new_pkg_data)
        
        # Assertions
        assert pkg_obj is not None  # New package should be created
        assert pkg_obj.derived_id == "pkgx/new-pkg"
        assert pkg_obj.name == "new-pkg" 
        assert pkg_obj.import_id == "new-pkg"
        assert pkg_obj.readme == "Brand new package"
        assert pkg_obj.package_manager_id == mock_config.pm_config.pm_id
        assert update_payload == {}  # No updates for new package
        
        # Test URL creation
        new_urls = {}
        with patch.object(diff, '_canonicalize_url', side_effect=lambda x: x), \
             patch.object(diff, '_get_homepage_url', return_value="https://github.com/example/new-pkg"), \
             patch.object(diff, '_is_github_url', return_value=True):
            
            resolved_urls = diff.diff_url("new-pkg", new_pkg_data, new_urls)
            new_links, updated_links = diff.diff_pkg_url(pkg_id, resolved_urls)
        
        # Should create URLs for homepage, source, and repository (GitHub)
        assert len(new_urls) >= 2  # At least source and homepage
        assert len(new_links) >= 2  # At least source and homepage links
        assert len(updated_links) == 0  # No existing links to update

    def test_no_changes_scenario(self, mock_config):
        """Test scenario where package exists but has no changes"""
        
        # Setup existing package
        existing_pkg_id = uuid4()
        existing_package = Package(
            id=existing_pkg_id,
            derived_id="pkgx/unchanged-pkg",
            name="unchanged-pkg",
            package_manager_id=mock_config.pm_config.pm_id,
            import_id="unchanged-pkg",
            readme="Unchanged description"
        )
        
        cache = Cache(
            package_map={"unchanged-pkg": existing_package},
            url_map={},
            package_urls={},
            dependencies={}
        )
        
        # Create package data with same description
        pkg_data = create_pkgx_package(description="Unchanged description")
        
        # Test the diff
        diff = PkgxDiff(mock_config, cache)
        pkg_id, pkg_obj, update_payload = diff.diff_pkg("unchanged-pkg", pkg_data)
        
        # Assertions
        assert pkg_id == existing_pkg_id
        assert pkg_obj is None  # No new package
        assert update_payload is None  # No changes

    def test_missing_dependency_handling(self, mock_config):
        """Test how missing dependencies are handled"""
        
        existing_pkg_id = uuid4()
        existing_package = Package(
            id=existing_pkg_id,
            derived_id="pkgx/missing-dep-pkg",
            name="missing-dep-pkg",
            import_id="missing-dep-pkg"
        )
        
        cache = Cache(
            package_map={"missing-dep-pkg": existing_package},
            url_map={},
            package_urls={},
            dependencies={}
        )
        
        # Create package with dependency that doesn't exist in cache
        pkg_data = create_pkgx_package(dependencies=["non-existent-dep"])
        
        diff = PkgxDiff(mock_config, cache)
        new_deps, removed_deps = diff.diff_deps("missing-dep-pkg", pkg_data)
        
        # Should handle gracefully - no deps added for missing packages
        assert len(new_deps) == 0
        assert len(removed_deps) == 0


if __name__ == "__main__":
    pytest.main([__file__])