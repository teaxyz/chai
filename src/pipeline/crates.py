from os import getenv

from dataclasses import dataclass

from src.pipeline.utils.crates.structures import URLTypes, UserTypes
from src.pipeline.utils.fetcher import TarballFetcher
from src.pipeline.utils.logger import Logger
from src.pipeline.utils.pg import DB
from src.pipeline.utils.crates.transformer import CratesTransformer

logger = Logger("crates_orchestrator")


# TODO: we can make a global version of this
@dataclass
class Config:
    file_location: str
    test: bool
    package_manager_id: str
    url_types: URLTypes
    user_types: UserTypes

    def __str__(self):
        return f"Config(file_location={self.file_location}, test={self.test}, \
            package_manager_id={self.package_manager_id}, url_types={self.url_types}, \
            user_types={self.user_types})"


def initialize(db: DB) -> Config:
    file_location = "https://static.crates.io/db-dump.tar.gz"
    test = getenv("TEST", "false").lower() == "true"
    package_manager = db.select_package_manager_by_name("crates", create=True)
    homepage_url = db.select_url_types_homepage(create=True)
    repository_url = db.select_url_types_repository(create=True)
    crates_source = db.select_source_by_name("crates", create=True)
    github_source = db.select_source_by_name("github", create=True)
    url_types = URLTypes(homepage=homepage_url.id, repository=repository_url.id)
    user_types = UserTypes(crates=crates_source.id, github=github_source.id)

    logger.debug("initialized config")

    return Config(
        file_location=file_location,
        test=test,
        package_manager_id=package_manager.id,
        url_types=url_types,
        user_types=user_types,
    )


def fetch(config: Config) -> None:
    fetcher = TarballFetcher("crates", config.file_location)
    files = fetcher.fetch()
    fetcher.write(files)


def load(db: DB, transformer: CratesTransformer, config: Config) -> None:
    # always inserts user and packages
    # db.insert_packages(transformer.packages(), config.package_manager_id, "crates")
    # db.insert_users(transformer.users())

    # # crates provides a gh_login for every single crate publisher
    # # this is the only user type we load, with the GitHub source as `source_id`
    # db.insert_user_packages(transformer.user_packages(), config.user_types.github)

    # db.insert_versions(transformer.versions())
    db.insert_user_versions(transformer.user_versions(), config.user_types.github)

    if not config.test:
        # these are bigger files, so we skip them in tests
        db.insert_dependencies(transformer.dependencies())

    db.insert_load_history(config.package_manager_id)
    logger.log("✅ crates")


def main(db: DB) -> None:
    config = initialize(db)
    logger.debug(config)
    fetch(config)

    transformer = CratesTransformer(config.url_types, config.user_types)
    load(db, transformer, config)

    coda = """
        validate by running 
        `psql "postgresql://postgres:s3cr3t@localhost:5435/chai" \
            -c "SELECT * FROM load_history;"`
    """
    logger.log(coda)


if __name__ == "__main__":
    main()
