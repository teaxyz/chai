from src.pipeline.utils.fetcher import TarballFetcher
from src.pipeline.utils.logger import Logger
from src.pipeline.utils.pg import DB
from src.pipeline.utils.crates.transformer import CratesTransformer

FILE_LOCATION = "https://static.crates.io/db-dump.tar.gz"

logger = Logger("crates_orchestrator", mode=Logger.VERBOSE)


# this is the orchestrator for crates
# in general, the orchestrators:
# 0. prep the db for the package manager, getting its id, and anything else
#    that needs to be inserted before we load data (url types, etc)
# 1. fetch data associated with that package manager
# 2. call the various transformer methods associated with that package manager
# 3. load the data into the db
def get_crates_packages(db: DB) -> None:
    # get crates's package manager id, insert it if it doesn't exist
    package_manager = db.select_package_manager_id("crates", create=True)

    # get the homepage and repository url types, insert them if they don't exist
    # homepage / repository because that is what crates provides
    homepage_url_type_id = db.select_url_types_homepages()
    if homepage_url_type_id is None:
        homepage_url_type_id = db.insert_url_types("homepage")
    logger.debug(f"homepage_url_type_id: {homepage_url_type_id}")

    repository_url_type_id = db.select_url_types_repositories()
    if repository_url_type_id is None:
        repository_url_type_id = db.insert_url_types("repository")
    logger.debug(f"repository_url_type_id: {repository_url_type_id}")

    # get the raw files
    logger.log("need to unravel a ~3GB tarball, this takes ~42 seconds")
    fetcher = TarballFetcher("crates", FILE_LOCATION)
    files = fetcher.fetch()
    fetcher.write(files)

    # use the transformer to figure out what we'd need for our ranker
    transformer = CratesTransformer(homepage_url_type_id, repository_url_type_id)
    logger.log("transforming crates packages")

    # load the projects, versions, and dependencies into our db
    logger.log("loading crates packages into db, currently this might take a while")

    # packages
    db.insert_packages(transformer.packages(), package_manager)

    # versions
    db.insert_versions(transformer.versions())

    # dependencies
    db.insert_dependencies(transformer.dependencies())

    # insert load history
    db.insert_load_history(package_manager.id)
    logger.log("✅ crates")
    logger.log("in a new terminal, run README.md/db-list-history to verify")
