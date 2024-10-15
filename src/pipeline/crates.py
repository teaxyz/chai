from os import getenv

from dataclasses import dataclass
import sys

from src.pipeline.utils.crates.structures import URLTypes, UserTypes
from src.pipeline.utils.fetcher import TarballFetcher
from src.pipeline.utils.logger import Logger
from src.pipeline.utils.pg import DB
from src.pipeline.utils.crates.transformer import CratesTransformer

logger = Logger("crates_orchestrator")


# TODO: global config class
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


def initialize(db: DB) -> Config:
    file_location = "https://static.crates.io/db-dump.tar.gz"
    test = getenv("TEST", "false").lower() == "true"
    fetch = getenv("FETCH", "true").lower() == "true"
    package_manager = db.select_package_manager_by_name("crates", create=True)
    homepage_url = db.select_url_types_homepage(create=True)
    repository_url = db.select_url_types_repository(create=True)
    documentation_url = db.select_url_types_documentation(create=True)
    crates_source = db.select_source_by_name("crates", create=True)
    github_source = db.select_source_by_name("github", create=True)
    url_types = URLTypes(
        homepage=homepage_url.id,
        repository=repository_url.id,
        documentation=documentation_url.id,
    )
    user_types = UserTypes(crates=crates_source.id, github=github_source.id)

    logger.debug("initialized config")

    return Config(
        file_location=file_location,
        test=test,
        fetch=fetch,
        package_manager_id=package_manager.id,
        url_types=url_types,
        user_types=user_types,
    )


def fetch(config: Config) -> None:
    fetcher = TarballFetcher("crates", config.file_location)
    files = fetcher.fetch()
    fetcher.write(files)


def load(db: DB, transformer: CratesTransformer, config: Config) -> None:
    logger.log("loading crates packages...this should take a minute")
    db.insert_packages(transformer.packages(), config.package_manager_id, "crates")
    logger.log("✅ inserted packages")

    logger.log("loading crates urls...this should take a minute")
    db.insert_urls(transformer.urls())
    logger.log("✅ inserted urls")

    logger.log("loading crates package urls...this should take ~3 minutes")
    db.insert_package_urls(transformer.package_urls())
    logger.log("✅ inserted package urls")

    logger.log("loading crates versions...this should take ~5 minutes")
    db.insert_versions(transformer.versions())
    logger.log("✅ inserted versions")

    logger.log("loading crates users...this should take a minute")
    db.insert_users(transformer.users(), config.user_types.crates)
    logger.log("✅ inserted users")

    logger.log("loading crates user packages...this should take a few seconds")
    db.insert_user_packages(transformer.user_packages())
    logger.log("✅ inserted user packages")

    if not config.test:
        # these are bigger files, so we skip them in tests
        logger.log("loading crates user versions...this should take ~5 minutes")
        db.insert_user_versions(transformer.user_versions(), config.user_types.github)
        logger.log("✅ inserted user versions")

        logger.log("loading crates dependencies...this should take ~1 hour")
        db.insert_dependencies(transformer.dependencies())
        logger.log("✅ inserted dependencies")

    db.insert_load_history(config.package_manager_id)
    logger.log("✅ crates")


def main(db: DB) -> None:
    config = initialize(db)
    logger.debug(config)
    if config.fetch:
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
