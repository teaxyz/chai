from typing import Set

from core.config import Config, PackageManager
from core.db import DB
from core.fetcher import TarballFetcher
from core.logger import Logger
from core.structs import CurrentGraph, CurrentURLs


class CratesDB(DB):
    def __init__(self, config: Config):
        super().__init__("crates_db")
        self.config = config
        self.get_current_graph()

    def get_current_graph(self) -> CurrentGraph:
        return self.current_graph(self.config.pm_config.pm_id)

    def get_current_urls(self, urls: Set[str]) -> CurrentURLs:
        return self.current_urls(urls)


def main(config: Config, db: CratesDB):
    logger = Logger("crates_main_v2")
    logger.log("Starting crates_main_v2")

    fetcher: TarballFetcher = TarballFetcher(
        "crates",
        config.pm_config.source,
        config.exec_config.no_cache,
        config.exec_config.test,
    )
    files = fetcher.fetch()

    if not config.exec_config.no_cache:
        logger.log("Writing files to disk")
        fetcher.write(files)

    # we should first do some standardization
    # go though crates, standardize URLs
    # grab latest version for each crate
    # grab that version's dependencies from dependency table
    # default_versions table has the latest
    # version_downloads
    # versions has all the URLs, as well...let's just pick one
    # anyway, all this has to happen in a Parser class

    # then, we can build the cache using whatever we got from the DB
    # and start the diff process

    logger.log("✅ Done")


if __name__ == "__main__":
    config = Config(PackageManager.CRATES)
    db = CratesDB(config)
    main(config, db)
