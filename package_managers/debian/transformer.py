from dataclasses import dataclass, field
from typing import Dict

from sqlalchemy.dialects.postgresql import UUID

from core.config import Config
from core.models import URL, DependsOn, Package, Version
from core.transformer import Transformer
from package_managers.debian.parser import DebianData, DebianParser


@dataclass
class Cache:
    package: Package
    versions: list[Version] = field(default_factory=list)
    urls: list[URL] = field(default_factory=list)
    dependency: list[DependsOn] = field(default_factory=list)


@dataclass
class TempVersion:
    version: str
    package_name: str
    import_id: str


@dataclass
class TempDependency:
    package_name: str
    dependency_name: str
    dependency_type_id: UUID
    semver_range: str


# the flow for debian should be to go through each file once, and build out all the data
# at once.
#
# this will allow us to have a more efficient pipeline, and will also allow us to
# have a more efficient database schema.


class DebianTransformer(Transformer):
    def __init__(self, config: Config):
        super().__init__("debian")
        self.package_manager_id = config.pm_config.pm_id
        self.url_types = config.url_types
        self.depends_on_types = config.dependency_types
        self.test = config.exec_config.test
        self.files = {"packages": "packages", "sources": "sources"}
        self.cache_map: Dict[str, Cache] = {}

    def summary(self) -> None:
        print("********* SUMMARY *********")
        print(f"Cache Map size: {len(self.cache_map)}")

        for i, (key, value) in enumerate(self.cache_map.items()):
            if i > 10:
                break
            print(f"{i}: {key} - {value}")

    # orchestrator
    def transform(self) -> None:
        source_file = self.files["sources"]
        sources_file = self.open(source_file)

        source_parser = DebianParser(sources_file, self.test)
        if self.test:
            self.logger.log("Testing mode enabled, only parsing 10 sources")

        for source in source_parser.parse():
            pkg_name = source.package
            binaries = source.binary
            homepage = source.homepage

            # generate the package
            package = self.generate_chai_package(source)

            # put it in the cache
            item = Cache(package=package)
            self.cache_map[pkg_name] = item
            self.cache_map[homepage] = item
            for binary in binaries:
                self.cache_map[binary] = item

            # now, manage the urls
            # sources file has the homepage url type
            homepage_url_type = self.url_types.homepage
            homepage_url = self.generate_chai_url(source, homepage_url_type)
            item.urls.append(homepage_url)

            # now, manage the versions
            version = self.generate_chai_version(source)
            item.versions.append(version)

            # finally, manage the dependencies
            dependencies = self.generate_chai_build_dependencies(source)
            item.dependency.extend(dependencies)

        # and the package file
        # TODO:
        # self.summary()

    def generate_chai_package(self, debian_data: DebianData) -> Package:
        internal_id = f"debian/{debian_data.package}"
        return Package(
            derived_id=internal_id,
            name=debian_data.package,
            package_manager_id=self.package_manager_id,
            import_id=internal_id,
            readme=debian_data.description,
        )

    def generate_chai_url(self, debian_data: DebianData, url_type_id: UUID) -> URL:
        homepage = self.canonicalize(debian_data.homepage)
        return URL(url=homepage, url_type_id=url_type_id)

    # For versions and packages however, I need a temporary structure to hold the data
    # until I can insert it into the database
    # I need the package ids
    def generate_chai_version(self, debian_data: DebianData) -> Version:
        version = debian_data.version
        internal_id = f"debian/{debian_data.package}/{version}"
        return TempVersion(
            version=version, package_name=debian_data.package, import_id=internal_id
        )

    def generate_chai_build_dependencies(
        self, debian_data: DebianData
    ) -> list[TempDependency]:
        dependencies = []
        for dependency in debian_data.build_depends:
            dependency_name = dependency.package
            semver = dependency.semver
            dependencies.append(
                TempDependency(
                    package_name=debian_data.package,
                    dependency_name=dependency_name,
                    dependency_type_id=self.depends_on_types.build,
                    semver_range=semver,
                )
            )
        return dependencies

    # this can only be run after the packages and versions have been inserted
    def convert_temp_dependencies(
        self, temp_dependencies: list[TempDependency]
    ) -> list[DependsOn]:
        """
        Convert the temporary dependencies into the final dependencies
        - Since dependencies are from a version_id to a package_id, both ids need to be
        loaded first
        - This function assumes that they live in the cache, and retrieve it from there
        """
        dependencies = []
        for temp_dependency in temp_dependencies:
            dependencies.append(
                DependsOn(
                    version_id=self.cache_map[temp_dependency.package_name]
                    .versions[0]
                    .id,
                    dependency_id=self.cache_map[
                        temp_dependency.dependency_name
                    ].package.id,
                    dependency_type_id=temp_dependency.dependency_type_id,
                    semver_range=temp_dependency.semver_range,
                )
            )
        return dependencies

    # this can only be run after the packages have been inserted
    def convert_temp_version(self, temp_version: TempVersion) -> Version:
        """
        Convert the temporary version into the final version
        - Since versions are from a package_id to a version, packages need to be loaded
        first
        - Once the package is loaded, the cache **must** be updated with its id
        """
        return Version(
            package_id=self.cache_map[temp_version.package_name].package.id,
            version=temp_version.version,
            import_id=temp_version.import_id,
        )
