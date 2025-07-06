from core.config import Config
from core.db import DB, CurrentURLs
from core.structs import CurrentGraph


class HomebrewDB(DB):
    def __init__(self, logger_name: str, config: Config):
        super().__init__(logger_name)
        self.config = config
        self.set_current_graph()

    def set_current_graph(self) -> None:
        """Get the Homebrew packages and dependencies"""
        self.graph: CurrentGraph = self.current_graph(self.config.pm_config.pm_id)
        self.logger.log(f"Loaded {len(self.graph.package_map)} Homebrew packages")

    def set_current_urls(self, urls: set[str]) -> None:
        """Wrapper for setting current urls"""
        self.urls: CurrentURLs = self.current_urls(urls)
        self.logger.log(f"Found {len(self.urls.url_map)} Homebrew URLs")
