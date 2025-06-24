#!/usr/bin/env pkgx uv run

from core.config import Config
from core.db import DB, CurrentURLs
from core.structs import CurrentGraph


class DebianDB(DB):
    def __init__(self, logger_name: str, config: Config):
        super().__init__(logger_name)
        self.config = config

    def set_current_graph(self) -> None:
        """Get the debian packages and dependencies"""
        self.graph: CurrentGraph = self.current_graph(self.config.pm_config.pm_id)
        self.logger.log(f"Loaded {len(self.graph.package_map)} debian packages")

    def set_current_urls(self, urls: set[str]) -> None:
        """Getting all the URLs and Package URLs from the database"""
        self.urls: CurrentURLs = self.current_urls(urls)
        self.logger.log(f"Loaded {len(self.urls.url_map)} URLs")
