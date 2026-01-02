#!/usr/bin/env pkgx uv run

from core.config import Config
from core.db import DB, CurrentURLs
from core.structs import CurrentGraph, DiffResult


class DebianDB(DB):
    def __init__(self, logger_name: str, config: Config):
        super().__init__(logger_name)
        self.config = config

    def set_current_graph(self) -> None:
        """Get the debian packages and dependencies"""
        self.graph: CurrentGraph = self.current_graph(self.config.pm_config.pm_id)

    def set_current_urls(self, urls: set[str]) -> None:
        """Getting all the URLs and Package URLs from the database"""
        self.urls: CurrentURLs = self.current_urls(urls)

    def ingest_wrapper(self, diff_result: DiffResult) -> None:
        """Wrapper for the main ingest function to handle DiffResult"""
        final_new_urls = list(diff_result.new_urls.values())
        self.ingest(
            diff_result.new_packages,
            final_new_urls,
            diff_result.new_package_urls,
            diff_result.new_deps,
            diff_result.removed_deps,
            diff_result.updated_packages,
            diff_result.updated_package_urls,
        )
