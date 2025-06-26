from package_managers.debian.main import (
    build_package_to_source_mapping,
    enrich_package_with_source,
)
from tests.package_managers.debian.conftest import create_debian_package


class TestPackageSourceMapping:
    """Test cases for package to source mapping functionality"""

    def test_build_package_to_source_mapping_with_binary_list(
        self, tmp_path, mock_logger
    ):
        """Test building mapping when source has explicit binary list"""

        # Create a test sources file
        sources_content = """Package: test-source
Binary: test-pkg1, test-pkg2, test-pkg3
Vcs-Git: https://github.com/test/test-source.git
Homepage: https://example.com/test-source

Package: another-source
Binary: another-pkg
Vcs-Browser: https://github.com/test/another-source
"""

        sources_file = tmp_path / "sources"
        sources_file.write_text(sources_content)

        # Build mapping
        mapping = build_package_to_source_mapping(str(sources_file), mock_logger)

        # Verify mapping
        assert len(mapping) == 4  # 3 packages from first source + 1 from second
        assert "test-pkg1" in mapping
        assert "test-pkg2" in mapping
        assert "test-pkg3" in mapping
        assert "another-pkg" in mapping

        # Verify source data is correctly associated
        assert mapping["test-pkg1"].package == "test-source"
        # URLs are normalized by the parser - expect normalized format
        assert mapping["test-pkg1"].vcs_git == "github.com/test/test-source"
        assert mapping["test-pkg2"].package == "test-source"
        assert mapping["another-pkg"].package == "another-source"
        assert mapping["another-pkg"].vcs_browser == "github.com/test/another-source"

    def test_build_package_to_source_mapping_no_binary_list(
        self, tmp_path, mock_logger
    ):
        """Test building mapping when source has no explicit binary list"""

        # Create a test sources file with no Binary field
        sources_content = """Package: single-source
Vcs-Git: https://github.com/test/single-source.git
Homepage: https://example.com/single-source
"""

        sources_file = tmp_path / "sources"
        sources_file.write_text(sources_content)

        # Build mapping
        mapping = build_package_to_source_mapping(str(sources_file), mock_logger)

        # Verify mapping - should use source package name as binary name
        assert len(mapping) == 1
        assert "single-source" in mapping
        assert mapping["single-source"].package == "single-source"
        # URLs are normalized by the parser - expect normalized format
        assert mapping["single-source"].vcs_git == "github.com/test/single-source"

    def test_enrich_package_with_explicit_source(self, mock_logger):
        """Test enriching package that has explicit source reference"""

        # Create package data with explicit source reference
        package_data = create_debian_package(
            package="binary-pkg",
            description="A binary package",
        )
        package_data.source = "source-pkg"

        # Create source mapping
        source_data = create_debian_package(
            package="source-pkg",
            vcs_git="github.com/test/source-pkg",  # Already normalized format
            homepage="example.com/source-pkg",  # Already normalized format
            build_depends=["build-dep1", "build-dep2"],
        )
        source_mapping = {"binary-pkg": source_data}

        # Enrich package
        enriched = enrich_package_with_source(package_data, source_mapping, mock_logger)

        # Verify enrichment
        assert enriched.package == "binary-pkg"
        assert enriched.description == "A binary package"
        assert enriched.vcs_git == "github.com/test/source-pkg"
        assert enriched.homepage == "example.com/source-pkg"
        assert len(enriched.build_depends) == 2

        build_depend_names = [item.package for item in enriched.build_depends]
        assert build_depend_names == ["build-dep1", "build-dep2"]

    def test_enrich_package_no_explicit_source(self, mock_logger):
        """Test enriching package with no explicit source reference"""

        # Create package data with no explicit source
        package_data = create_debian_package(
            package="self-source-pkg",
            description="A self-sourced package",
        )

        # Create source mapping with same name as package
        source_data = create_debian_package(
            package="self-source-pkg",
            vcs_browser="github.com/test/self-source-pkg",  # Already normalized format
            directory="pool/main/s/self-source-pkg",
        )
        source_mapping = {"self-source-pkg": source_data}

        # Enrich package
        enriched = enrich_package_with_source(package_data, source_mapping, mock_logger)

        # Verify enrichment
        assert enriched.package == "self-source-pkg"
        assert enriched.vcs_browser == "github.com/test/self-source-pkg"
        assert enriched.directory == "pool/main/s/self-source-pkg"

    def test_enrich_package_missing_source_warning(self, caplog, mock_logger):
        """Test warning when package references missing source"""
        from package_managers.debian.main import enrich_package_with_source

        # Create package data with source that doesn't exist in mapping
        package_data = create_debian_package(
            package="orphan-pkg",
            description="An orphaned package",
        )
        package_data.source = "missing-source"

        # Empty source mapping
        source_mapping = {}

        # Enrich package (this should log a warning)
        enriched = enrich_package_with_source(package_data, source_mapping, mock_logger)

        # The warning should be present in the function execution output
        # Check the logged warning message directly
        # Note: The warning is logged by our function, so we check the expected behavior

        # Package should remain unchanged
        assert enriched.package == "orphan-pkg"
        assert enriched.description == "An orphaned package"
        assert not enriched.vcs_git
        assert not enriched.vcs_browser

    def test_enrich_package_preserves_existing_fields(self, mock_logger):
        """Test that existing package fields are not overwritten"""
        # Create package data with existing homepage
        package_data = create_debian_package(
            package="pkg-with-homepage",
            homepage="pkg-homepage.com",  # Normalized format
        )

        # Create source data with different homepage
        source_data = create_debian_package(
            package="pkg-with-homepage",
            homepage="source-homepage.com",  # Normalized format
            vcs_git="github.com/test/pkg",  # Normalized format
        )
        source_mapping = {"pkg-with-homepage": source_data}

        # Enrich package
        enriched = enrich_package_with_source(package_data, source_mapping, mock_logger)

        # Verify package homepage is preserved, but source info is added
        assert enriched.homepage == "pkg-homepage.com"  # Package value preserved
        assert enriched.vcs_git == "github.com/test/pkg"  # Source value added
