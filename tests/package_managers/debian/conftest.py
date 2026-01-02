from package_managers.debian.parser import DebianData, Depends


def create_debian_package(
    package: str = "test-package",
    description: str = "Test package",
    homepage: str = "",
    vcs_git: str = "",
    vcs_browser: str = "",
    directory: str = "",
    filename: str = "",
    depends: list[str] | None = None,
    build_depends: list[str] | None = None,
    recommends: list[str] | None = None,
    suggests: list[str] | None = None,
) -> DebianData:
    """Helper to create DebianData instances for testing"""

    debian_data = DebianData()
    debian_data.package = package
    debian_data.description = description
    debian_data.homepage = homepage
    debian_data.vcs_git = vcs_git
    debian_data.vcs_browser = vcs_browser
    debian_data.directory = directory
    debian_data.filename = filename

    # Convert string dependencies to Depends objects
    if depends:
        debian_data.depends = [Depends(package=dep, semver="*") for dep in depends]
    if build_depends:
        # build_depends is now list[Depends] like other dependency fields
        debian_data.build_depends = [
            Depends(package=dep, semver="*") for dep in build_depends
        ]
    if recommends:
        debian_data.recommends = [
            Depends(package=dep, semver="*") for dep in recommends
        ]
    if suggests:
        debian_data.suggests = [Depends(package=dep, semver="*") for dep in suggests]

    return debian_data
