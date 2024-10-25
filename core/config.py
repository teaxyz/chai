from enum import Enum

from sqlalchemy import UUID

from core.db import DB
from core.logger import Logger
from core.utils import env_vars

logger = Logger("config")


class PackageManager(Enum):
    CRATES = "crates"
    HOMEBREW = "homebrew"


TEST = env_vars("TEST", "false")
FETCH = env_vars("FETCH", "true")
NO_CACHE = env_vars("NO_CACHE", "true")
SOURCES = {
    PackageManager.CRATES: "https://static.crates.io/db-dump.tar.gz",
    PackageManager.HOMEBREW: "https://github.com/Homebrew/homebrew-core/tree/master/Formula",  # noqa
}

# The three configuration values URLTypes, DependencyTypes, and UserTypes will query the
# DB to get the respective IDs. If the values don't exist in the database, they will
# raise an AttributeError (None has no attribute id) at the start


class ExecConf:
    test: bool
    fetch: bool
    no_cache: bool

    def __init__(self) -> None:
        self.test = TEST
        self.fetch = FETCH
        self.no_cache = NO_CACHE

    def __str__(self):
        return f"ExecConf(test={self.test},fetch={self.fetch},no_cache={self.no_cache}"


class PMConf:
    pm_id: str
    source: str

    def __init__(self, pm: PackageManager, db: DB):
        self.pm_id = db.select_package_manager_by_name(pm.value).id
        self.source = SOURCES[pm]

    def __str__(self):
        return f"PMConf(pm_id={self.pm_id},source={self.source})"


class URLTypes:
    homepage: UUID
    repository: UUID
    documentation: UUID
    source: UUID

    def __init__(self, db: DB):
        self.load_url_types(db)

    def load_url_types(self, db: DB) -> None:
        self.homepage = db.select_url_types_homepage().id
        self.repository = db.select_url_types_repository().id
        self.documentation = db.select_url_types_documentation().id
        self.source = db.select_url_types_source().id

    def __str__(self) -> str:
        return f"URLs(homepage={self.homepage},repo={self.repository},docs={self.documentation},src={self.source})"  # noqa


class UserTypes:
    crates: UUID
    github: UUID

    def __init__(self, db: DB):
        self.crates = db.select_source_by_name("crates").id
        self.github = db.select_source_by_name("github").id

    def __str__(self) -> str:
        return f"UserTypes(crates={self.crates},github={self.github})"


class DependencyTypes:
    build: UUID
    development: UUID
    runtime: UUID
    test: UUID
    optional: UUID
    recommended: UUID

    def __init__(self, db: DB):
        self.build = db.select_dependency_type_by_name("build").id
        self.development = db.select_dependency_type_by_name("development").id
        self.runtime = db.select_dependency_type_by_name("runtime").id
        self.test = db.select_dependency_type_by_name("test").id
        self.optional = db.select_dependency_type_by_name("optional").id
        self.recommended = db.select_dependency_type_by_name("recommended").id

    def __str__(self) -> str:
        return f"DependencyTypes(build={self.build},development={self.development},runtime={self.runtime},test={self.test},optional={self.optional},recommended={self.recommended})"  # noqa


class Config:
    exec_config: ExecConf
    pm_config: PMConf
    url_types: URLTypes
    user_types: UserTypes
    dependency_types: DependencyTypes

    def __init__(self, pm: PackageManager, db: DB) -> None:
        self.exec_config = ExecConf()
        self.pm_config = PMConf(pm, db)
        self.url_types = URLTypes(db)
        self.user_types = UserTypes(db)
        self.dependency_types = DependencyTypes(db)

    def __str__(self):
        return f"Config(exec_config={self.exec_config}, pm_config={self.pm_config}, url_types={self.url_types}, user_types={self.user_types}, dependency_types={self.dependency_types})"  # noqa


if __name__ == "__main__":
    print(PackageManager.CRATES.value)
