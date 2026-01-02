from core.logger import Logger
from package_managers.debian.parser import DebianParser
from package_managers.debian.structs import DebianData


def build_package_to_source_mapping(
    sources_file_path: str, logger: Logger
) -> dict[str, DebianData]:
    """
    Build a mapping from binary package names to their source information.

    Args:
        sources_file_path: Path to the sources file
        test: Whether to limit parsing for testing

    Returns:
        Dictionary mapping binary package names to source DebianData objects
    """
    # Parse sources file
    with open(sources_file_path) as f:
        sources_content = f.read()
    sources_parser = DebianParser(sources_content)

    # Build mapping: binary_package_name -> source_debian_data
    package_to_source: dict[str, DebianData] = {}

    for source_data in sources_parser.parse():
        # Each source may produce multiple binary packages
        if source_data.binary:
            # Source has explicit binary list
            for binary_name in source_data.binary:
                binary_name = binary_name.strip()
                if binary_name:
                    package_to_source[binary_name] = source_data
        else:
            # No explicit binary list, assume source name == binary name
            if source_data.package:
                package_to_source[source_data.package] = source_data

    logger.log(
        f"Built mapping for {len(package_to_source)} binary packages from sources"
    )
    return package_to_source


def enrich_package_with_source(
    package_data: DebianData, source_mapping: dict[str, DebianData], logger: Logger
) -> DebianData:
    """
    Enrich a package with its corresponding source information.

    Args:
        package_data: The package data from packages file
        source_mapping: Mapping from package names to source data

    Returns:
        Enriched DebianData with both package and source information
    """
    # Start with the package data
    enriched = package_data

    # Determine source name
    binary_name = package_data.package

    # Look up source information
    if binary_name in source_mapping:
        source_data = source_mapping[binary_name]

        # Enrich package with source information
        # Only add source fields that aren't already populated
        if not enriched.vcs_browser and source_data.vcs_browser:
            enriched.vcs_browser = source_data.vcs_browser
        if not enriched.vcs_git and source_data.vcs_git:
            enriched.vcs_git = source_data.vcs_git
        if not enriched.directory and source_data.directory:
            enriched.directory = source_data.directory
        if not enriched.build_depends and source_data.build_depends:
            enriched.build_depends = source_data.build_depends
        if not enriched.homepage and source_data.homepage:
            enriched.homepage = source_data.homepage

    else:
        # Log warning for missing source
        source_name = package_data.source or package_data.package
        logger.warn(
            f"Binary '{binary_name}' of source '{source_name}' was not found in sources file"
        )

    return enriched
