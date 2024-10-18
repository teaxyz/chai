from dataclasses import dataclass
from enum import Enum
from os import getenv

from core.db import DB
from core.logger import Logger
from core.structs import URLTypes, UserTypes

logger = Logger("config")

TEST = getenv("TEST", "false").lower() == "true"
FETCH = getenv("FETCH", "true").lower() == "true"


class PackageManager(Enum):
    CRATES = "crates"
    HOMEBREW = "homebrew"


class Sources:
    crates: str = "https://static.crates.io/db-dump.tar.gz"
    homebrew: str = "https://github.com/Homebrew/homebrew-core/tree/master/Formula"


@dataclass
class Config:
    file_location: str
    test: bool
    fetch: bool
    package_manager_id: str
    url_types: URLTypes
    user_types: UserTypes

    def __str__(self):
        return f"Config(file_location={self.file_location}, test={self.test}, \
            fetch={self.fetch}, package_manager_id={self.package_manager_id}, \
            url_types={self.url_types}, user_types={self.user_types})"


def load_url_types(db: DB) -> URLTypes:
    logger.debug("loading url types, and creating if not exists")
    homepage_url = db.select_url_types_homepage(create=True)
    repository_url = db.select_url_types_repository(create=True)
    documentation_url = db.select_url_types_documentation(create=True)
    return URLTypes(
        homepage=homepage_url.id,
        repository=repository_url.id,
        documentation=documentation_url.id,
    )


def load_user_types(db: DB) -> UserTypes:
    logger.debug("loading user types, and creating if not exists")
    crates_source = db.select_source_by_name("crates", create=True)
    github_source = db.select_source_by_name("github", create=True)
    return UserTypes(
        crates=crates_source.id,
        github=github_source.id,
    )


def initialize(package_manager: PackageManager, db: DB) -> Config:
    url_types = load_url_types(db)
    user_types = load_user_types(db)

    if package_manager == PackageManager.CRATES:
        return Config(
            file_location=Sources.crates,
            test=False,
            fetch=True,
            package_manager_id=PackageManager.CRATES.value,
            url_types=url_types,
            user_types=user_types,
        )
    elif package_manager == PackageManager.HOMEBREW:
        return Config(
            file_location=Sources.homebrew,
            test=False,
            fetch=True,
            package_manager_id=PackageManager.HOMEBREW.value,
            url_types=url_types,
            user_types=user_types,
        )
