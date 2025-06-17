from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from core.logger import Logger
from core.utils import convert_keys_to_snake_case

logger = Logger("pkgx")
PROJECTS_DIR = "projects"
PACKAGE_FILE = "package.yml"

# IMPORTANT:
# the package.yml maintains a warnings list, which sometimes contain "vendored"
# this correlates to Homebrew's casks, and CHAI ignores them


# structures
# this enables everything, but we don't need all of it right now
@dataclass
class Distributable:
    url: str
    strip_components: int | None = field(default=None)
    ref: str | None = field(default=None)
    sig: str | None = field(default=None)
    sha: str | None = field(default=None)


@dataclass
class Version:
    github: str | None = field(default=None)  # (user)?(/tags/releases)
    gitlab: str | None = field(default=None)  # (user|project)?(/tags/releases)
    url: str | None = field(default=None)  # for non github projects
    match: str | None = field(default=None)  # regex to match the version
    strip: str | None = field(default=None)  # regex to strip the version
    ignore: str | None = field(default=None)  # regex to ignore the version
    versions: list[str] | None = field(default=None)  # list of versions
    npm: str | None = field(default=None)  # npm package name
    transform: str | None = field(default=None)  # regex to transform the version
    stripe: str | None = field(default=None)  # not sure what this is


@dataclass
class Dependency:
    name: str
    semver: str


@dataclass
class EnvironmentVariable:
    name: str
    value: str | list[str]


@dataclass
class DependencyBlock:
    platform: str  # 'all', 'linux', 'darwin', etc.
    dependencies: list[Dependency]


@dataclass
class Build:
    script: str
    dependencies: list[DependencyBlock] = field(default_factory=list)
    env: list[EnvironmentVariable] = field(default_factory=list)
    working_directory: str | None = field(default=None)


@dataclass
class Test:
    script: str
    dependencies: list[DependencyBlock] = field(default_factory=list)
    env: list[EnvironmentVariable] = field(default_factory=list)
    fixture: str | None = field(default=None)


@dataclass
class PkgxPackage:
    distributable: list[Distributable]
    versions: Version
    build: Build | None = field(default=None)
    test: Test | None = field(default=None)
    # provides: list[str] = field(default_factory=list)  # all cli commands provided
    # platforms: list[str] = field(
    #     default_factory=list
    # )  # darwin, linux/x64, linux/arm64, etc.
    # Store a list of dependency blocks, each specifying a platform and its deps
    dependencies: list[DependencyBlock] = field(default_factory=list)


# Pkgx Parser can look at the pantry and yield a dictionary of information in the YAML
class PkgxParser:
    def __init__(self, repo_path: str):
        self.repo_path = repo_path

    def find_package_yamls(self) -> Iterator[tuple[Path, str]]:
        """Finds all package.yml files within the projects directory."""
        projects_path = Path(self.repo_path) / PROJECTS_DIR
        if not projects_path.is_dir():
            logger.error(f"Projects directory not found at: {projects_path}")
            return

        logger.debug(f"Searching for {PACKAGE_FILE} in {projects_path}...")
        count = 0
        for yaml_path in projects_path.rglob(PACKAGE_FILE):
            if yaml_path.is_file():
                # Calculate relative path for project identifier
                relative_path = yaml_path.parent.relative_to(projects_path)
                project_identifier = str(relative_path)
                yield yaml_path, project_identifier
                count += 1
        logger.debug(f"Found {count} {PACKAGE_FILE} files.")

    def is_vendored(self, data: dict[str, Any]) -> bool:
        """Checks if the package is vendored."""
        if "warnings" in data:
            warnings = data.get("warnings", [])
            if "vendored" in warnings:
                return True
        return False

    def parse_package_yaml(self, file_path: Path) -> PkgxPackage | None:
        """Parses a single package.yaml file."""
        try:
            with open(file_path) as f:
                data = yaml.safe_load(f)
                if not isinstance(data, dict):
                    logger.warn(
                        f"Expected dict, got {type(data).__name__} in {file_path}"
                    )
                    return None

                # check if the package is vendored
                if self.is_vendored(data):
                    return None

                pkgx_package = self.map_package_yaml_to_pkgx_package(
                    data, str(file_path)
                )
                return pkgx_package
        except yaml.YAMLError as e:
            logger.error(f"Error parsing YAML file {file_path}: {e}")
            return None
        except Exception as e:
            logger.error(f"Error reading file {file_path}: {e}")
            raise e
            return None

    def parse_packages(self) -> Iterator[tuple[PkgxPackage, str]]:
        """Parses all package.yml files found in the repository."""
        for yaml_path, project_identifier in self.find_package_yamls():
            parsed_data = self.parse_package_yaml(yaml_path)
            if parsed_data:
                yield parsed_data, project_identifier

    def _parse_dependency_list(
        self, deps_data: Any, context: str
    ) -> list[DependencyBlock]:
        """Parses a dependency dictionary into a list of DependencyBlock objects."""
        if not isinstance(deps_data, dict):
            # For now, assume empty dict means no deps, but non-dict is error.
            if deps_data is None or deps_data == {}:
                return []
            dep_type = type(deps_data).__name__
            raise TypeError(
                f"Expected dependencies to be a dict in {context}, got {dep_type}"
            )

        dependency_blocks = []
        direct_deps = []

        for key, value in deps_data.items():
            # Platform-specific block
            if isinstance(value, dict):
                platform = key
                platform_deps = []
                for dep_name, semver in value.items():
                    if isinstance(semver, str):
                        platform_deps.append(Dependency(name=dep_name, semver=semver))
                    elif isinstance(semver, int | float):
                        platform_deps.append(
                            Dependency(name=dep_name, semver=str(semver))
                        )
                    else:
                        raise TypeError(
                            f"Unexpected semver type for {dep_name} under platform {platform} in {context}: {type(semver).__name__}"
                        )
                if platform_deps:
                    dependency_blocks.append(
                        DependencyBlock(platform=platform, dependencies=platform_deps)
                    )
                # else: empty platform block is ignored

            # Direct dependency declaration
            elif isinstance(value, str):
                dep_name = key
                semver = value
                direct_deps.append(Dependency(name=dep_name, semver=semver))

            # Direct declaration, but sometimes the semvers are exact
            elif isinstance(value, int | float):
                dep_name = key
                semver = str(value)
                direct_deps.append(Dependency(name=dep_name, semver=semver))

            # Invalid structure
            else:
                raise TypeError(
                    f"Unexpected dependency value type for key '{key}' in {context}: {type(value).__name__}. Expected dict or str or float."
                )

        # Add all direct dependencies under the 'all' platform
        if direct_deps:
            dependency_blocks.append(
                DependencyBlock(platform="all", dependencies=direct_deps)
            )

        return dependency_blocks

    def _parse_build_section(self, build_data: Any, file_path_str: str) -> Build:
        """Parses the build section if its a dict, list, or str"""
        if isinstance(build_data, dict):
            # Pass original dependencies dict, don't convert keys here
            build_deps_list = self._parse_dependency_list(
                build_data.get("dependencies"), f"build section of {file_path_str}"
            )
            # Convert env var keys just before instantiation
            build_env = [
                EnvironmentVariable(**convert_keys_to_snake_case(env))
                for env in build_data.get("env", [])
                if isinstance(env, dict)
            ]
            # Convert build_data keys just before creating Build object
            build_kwargs = convert_keys_to_snake_case(build_data)
            return Build(
                script=build_kwargs.get("script", ""),
                dependencies=build_deps_list,  # Use the originally parsed list
                env=build_env,
                working_directory=build_kwargs.get("working_directory"),
            )
        elif isinstance(build_data, list):
            # Generally, it's a list of build commands, so we only have script info
            # TODO: Potentially improve handling of list-based build data
            script = (
                build_data[0] if build_data and isinstance(build_data[0], str) else ""
            )
            return Build(
                script=script,
                dependencies=[],
                env=[],
                working_directory=None,
            )
        elif isinstance(build_data, str):
            return Build(
                script=build_data,
                dependencies=[],
                env=[],
                working_directory=None,
            )
        else:
            build_type = type(build_data).__name__
            raise TypeError(f"Build in {file_path_str} is {build_type}")

    def _parse_test_section(self, test_data: Any, file_path_str: str) -> Test:
        """Parses the test section if its a dict, list, or str"""
        if isinstance(test_data, dict):
            # Pass original dependencies dict
            test_deps_list = self._parse_dependency_list(
                test_data.get("dependencies"), f"test section of {file_path_str}"
            )
            # Convert env var keys just before instantiation
            test_env = [
                EnvironmentVariable(**convert_keys_to_snake_case(env))
                for env in test_data.get("env", [])
                if isinstance(env, dict)
            ]
            # Convert test_data keys just before creating Test object
            test_kwargs = convert_keys_to_snake_case(test_data)
            return Test(
                script=test_kwargs.get("script", ""),
                dependencies=test_deps_list,  # Use the originally parsed list
                env=test_env,
                fixture=test_kwargs.get("fixture"),
            )
        elif isinstance(test_data, list):
            # TODO: Clarify how to handle list-based test data. Assuming empty for now.
            return Test(script="", dependencies=[], env=[], fixture=None)
        elif isinstance(test_data, str):
            # Assuming string directly means the script
            return Test(script=test_data, dependencies=[], env=[], fixture=None)
        elif isinstance(test_data, bool):
            # bad tests are sometimes just true/false
            return Test(script=str(test_data), dependencies=[], env=[], fixture=None)
        else:
            test_type = type(test_data).__name__
            raise TypeError(f"Test for {file_path_str} is {test_type}")

    def _parse_versions_section(
        self, versions_data: Any, file_path_str: str
    ) -> Version:
        """Parses the versions section if its a list, dict, or None"""
        if isinstance(versions_data, list):
            # list of version strings (nums)
            return Version(versions=versions_data)
        elif isinstance(versions_data, dict):
            # github or gitlab...something useful
            # Convert keys just before creating Version object
            return Version(**convert_keys_to_snake_case(versions_data))
        elif versions_data is None:
            # Handle case where versions might be missing, return default empty
            logger.warn(f"Missing 'versions' section in {file_path_str} using default.")
            return Version()
        else:
            version_type = type(versions_data).__name__
            raise TypeError(f"Versions in {file_path_str} is {version_type}")

    def _parse_distributable_section(
        self, distributable_data: Any, file_path_str: str
    ) -> Distributable | list[Distributable]:
        """Parses the distributable section from the package data."""
        if isinstance(distributable_data, list):
            # Convert keys for each dict in the list before creating Distributable
            return [
                Distributable(**convert_keys_to_snake_case(d))
                for d in distributable_data
                if isinstance(d, dict)
            ]
        elif isinstance(distributable_data, dict):
            # Convert keys just before creating Distributable object
            return [Distributable(**convert_keys_to_snake_case(distributable_data))]
        elif distributable_data is None:
            return [Distributable(url="~")]
        else:
            distributable_type = type(distributable_data).__name__
            raise TypeError(f"Distributable in {file_path_str} is {distributable_type}")

    def map_package_yaml_to_pkgx_package(
        self, data: dict[str, Any], file_path_str: str
    ) -> PkgxPackage:
        """Maps a package.yml to a PkgxPackage."""
        # Keep the original data, do not normalize globally here
        # normalized_data = convert_keys_to_snake_case(data)

        # Parse sections using helper functions, passing original data segments
        build_data = data.get("build")
        build_obj = self._parse_build_section(build_data, file_path_str)

        test_data = data.get("test")
        test_obj = self._parse_test_section(test_data, file_path_str)

        versions_data = data.get("versions")
        versions_obj = self._parse_versions_section(versions_data, file_path_str)

        distributable_data = data.get("distributable")
        distributable_obj = self._parse_distributable_section(
            distributable_data, file_path_str
        )

        # Parse top-level dependencies using original keys
        dependencies_data = data.get("dependencies")
        top_level_deps_list = self._parse_dependency_list(
            dependencies_data, f"top-level of {file_path_str}"
        )

        # TODO: Implement parsing for 'provides' list
        # would be useful because we have the set of "names" / "commands" for it!
        # provides_data = data.get("provides")
        # provides_obj = self._parse_provides_section(provides_data, file_path_str)

        # TODO: Implement parsing for 'platforms' list
        # platforms_data = data.get("platforms")
        # platforms_obj = self._parse_platforms_section(platforms_data, file_path_str)

        # Note: PkgxPackage itself doesn't directly take snake_case kwargs from top level
        # Its arguments are constructed from the parsed objects.
        return PkgxPackage(
            distributable=distributable_obj,
            versions=versions_obj,
            dependencies=top_level_deps_list,
            build=build_obj,
            test=test_obj,
            # provides=provides,
            # platforms=platforms,
        )
