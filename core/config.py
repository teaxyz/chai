from dataclasses import dataclass
from os import getenv

from core.db import DB
from core.logger import Logger
from core.structs import (
    DependencyTypes,
    PackageManager,
    PackageManagerIDs,
    Sources,
    URLTypes,
    UserTypes,
)

logger = Logger("config")

TEST = getenv("TEST", "false").lower() == "true"
FETCH = getenv("FETCH", "true").lower() == "true"


@dataclass
class Config:
    file_location: str
    test: bool
    fetch: bool
    package_manager_id: str
    url_types: URLTypes
    user_types: UserTypes
    dependency_types: DependencyTypes

    def __str__(self):
        return f"Config(file_location={self.file_location}, test={self.test}, \
            fetch={self.fetch}, package_manager_id={self.package_manager_id}, \
            url_types={self.url_types}, user_types={self.user_types}, \
            dependency_types={self.dependency_types})"


def load_url_types(db: DB) -> URLTypes:
    logger.debug("loading url types, and creating if not exists")
    homepage_url = db.select_url_types_homepage(create=True)
    repository_url = db.select_url_types_repository(create=True)
    documentation_url = db.select_url_types_documentation(create=True)
    source_url = db.select_url_types_source(create=True)
    return URLTypes(
        homepage=homepage_url.id,
        repository=repository_url.id,
        documentation=documentation_url.id,
        source=source_url.id,
    )


def load_user_types(db: DB) -> UserTypes:
    logger.debug("loading user types, and creating if not exists")
    crates_source = db.select_source_by_name("crates", create=True)
    github_source = db.select_source_by_name("github", create=True)
    return UserTypes(
        crates=crates_source.id,
        github=github_source.id,
    )


def load_package_manager_ids(db: DB) -> PackageManagerIDs:
    logger.debug("loading package manager ids, and creating if not exists")
    crates_package_manager = db.select_package_manager_by_name("crates", create=True)
    homebrew_package_manager = db.select_package_manager_by_name(
        "homebrew", create=True
    )
    return {
        PackageManager.CRATES: crates_package_manager.id,
        PackageManager.HOMEBREW: homebrew_package_manager.id,
    }


def load_dependency_types(db: DB) -> DependencyTypes:
    logger.debug("loading dependency types, and creating if not exists")
    build_dep_type = db.select_dependency_type_by_name("build", create=True)
    dev_dep_type = db.select_dependency_type_by_name("development", create=True)
    runtime_dep_type = db.select_dependency_type_by_name("runtime", create=True)
    test_dep_type = db.select_dependency_type_by_name("test", create=True)
    optional_dep_type = db.select_dependency_type_by_name("optional", create=True)
    recommended_dep_type = db.select_dependency_type_by_name("recommended", create=True)
    return DependencyTypes(
        build=build_dep_type.id,
        development=dev_dep_type.id,
        runtime=runtime_dep_type.id,
        test=test_dep_type.id,
        optional=optional_dep_type.id,
        recommended=recommended_dep_type.id,
    )


def load_sources() -> Sources:
    return {
        PackageManager.CRATES: "https://static.crates.io/db-dump.tar.gz",
        PackageManager.HOMEBREW: (
            "https://github.com/Homebrew/homebrew-core/tree/master/Formula"
        ),
    }


def initialize(package_manager: PackageManager, db: DB) -> Config:
    url_types = load_url_types(db)
    user_types = load_user_types(db)
    package_manager_ids = load_package_manager_ids(db)
    dependency_types = load_dependency_types(db)
    sources = load_sources()

    if package_manager == PackageManager.CRATES:
        return Config(
            file_location=sources[PackageManager.CRATES],
            test=False,
            fetch=True,
            package_manager_id=package_manager_ids[PackageManager.CRATES],
            url_types=url_types,
            user_types=user_types,
            dependency_types=dependency_types,
        )
    elif package_manager == PackageManager.HOMEBREW:
        return Config(
            file_location=sources[PackageManager.HOMEBREW],
            test=False,
            fetch=True,
            package_manager_id=package_manager_ids[PackageManager.HOMEBREW],
            url_types=url_types,
            user_types=user_types,
            dependency_types=dependency_types,
        )
