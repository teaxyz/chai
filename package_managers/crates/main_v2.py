import csv
from collections.abc import Generator

from core.config import Config, PackageManager
from core.db import DB
from core.fetcher import TarballFetcher
from core.logger import Logger
from core.structs import CurrentGraph, CurrentURLs
from core.transformer import Transformer
from core.utils import is_github_url
from package_managers.crates.structs import Crate, CrateLatestVersion, CrateUser


class CratesDB(DB):
    def __init__(self, config: Config):
        super().__init__("crates_db")
        self.config = config
        # self.set_current_graph()

    def set_current_graph(self) -> None:
        self.graph: CurrentGraph = self.current_graph(self.config.pm_config.pm_id)
        self.logger.log(f"Loaded {len(self.graph.package_map)} Crates packages")

    def set_current_urls(self, urls: set[str]) -> None:
        self.urls: CurrentURLs = self.current_urls(urls)
        self.logger.log(f"Found {len(self.urls.url_map)} Crates URLs")


class CratesTransformer(Transformer):
    def __init__(self, config: Config):
        super().__init__("crates")
        self.config = config

        # maps that we'd need
        self.crates: dict[int, Crate] = {}

        # files we need to parse
        self.files: dict[str, str] = {
            "crates": "crates.csv",
            "latest_versions": "default_versions.csv",
            "versions": "versions.csv",
            "dependencies": "dependencies.csv",
            "users": "users.csv",
            "teams": "teams.csv",
        }

    def _open_csv(self, file_name: str) -> Generator[dict[str, str], None, None]:
        try:
            file_path = self.finder(self.files[file_name])
            with open(file_path, newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    yield row
        except KeyError:
            raise KeyError(f"Missing {file_name} from self.files: {self.files}")
        except FileNotFoundError:
            self.logger.error(f"Missing {file_path} from data directory")
            raise FileNotFoundError(f"Missing {file_path} file")
        except Exception as e:
            self.logger.error(f"Error reading {file_path}: {e}")
            raise e

    def parse(self, file_name: str) -> Crate:
        # first go through crates.csv to
        # here, we can get the import_id, name, homepage, documentation, repository
        # and also source, from repo if it is like GitHub
        for i, row in enumerate(self._open_csv("crates")):
            crate_id = int(row["id"])
            name = row["name"]
            readme = row["readme"]

            # URLs:
            homepage = self.canonicalize(row["homepage"])
            documentation = self.canonicalize(row["documentation"])
            repository = self.canonicalize(row["repository"])

            source: str | None = None
            if is_github_url(repository):
                source = repository

            crate = Crate(
                crate_id, name, readme, homepage, repository, documentation, source
            )
            self.crates[crate_id] = crate

            if self.config.exec_config.test and i > 100:
                break

        self.logger.log(f"Parsed {len(self.crates)} crates")

        # get the map of crate_id to latest_version_id, and the set of all
        # latest_version_ids
        latest_versions: set[int]
        latest_versions = self._load_latest_versions()
        self.logger.log(f"Loaded {len(latest_versions)} latest versions")

        # now, iterate through the versions.csv, and populate LatestVersion objects,
        # only if the version_id is in the latest_versions set
        for row in self._open_csv("versions"):
            version_id = int(row["id"])
            crate_id = int(row["crate_id"])

            # ignore if this version is not the latest
            if version_id not in latest_versions:
                continue

            if crate_id not in self.crates.keys() and self.config.exec_config.test:
                # in test mode, we only look at a few crates
                continue
            elif crate_id not in self.crates.keys():
                # should never run into this
                raise ValueError(f"Crate {crate_id} not found in self.crates")

            checksum = row["checksum"]
            downloads = int(row["downloads"])
            license = row["license"]
            num = row["num"]
            published_at = row["created_at"]

            # make a CrateUser object from the published_by
            published_by_user = CrateUser(id=row["published_by"])

            latest_version = CrateLatestVersion(
                version_id,
                checksum,
                downloads,
                license,
                num,
                published_by_user,
                published_at,
            )

            # map this LatestVersion to the crate in self.crates
            self.crates[crate_id].latest_version = latest_version

        latest_version_count: int = 0
        for crate in self.crates.values():
            if crate.latest_version is not None:
                latest_version_count += 1

        self.logger.log(f"Found {latest_version_count} latest versions")

    def _load_latest_versions(self) -> set[int]:
        latest_versions: set[int] = set()
        for row in self._open_csv("latest_versions"):
            version_id = int(row["version_id"])
            latest_versions.add(version_id)

        return latest_versions

    def _load_users(self) -> dict[int, CrateUser]:
        users: dict[int, CrateUser] = {}
        for row in self._open_csv("users"):
            user_id = int(row["id"])
            name = row["name"]
            github_username = row["github_username"]
            user = CrateUser(user_id, name, github_username)
            users[user_id] = user

        self.logger.log(f"Loaded {len(users)} users")
        return users


def main(config: Config, db: CratesDB):
    logger = Logger("crates_main_v2")
    logger.log("Starting crates_main_v2")

    fetcher: TarballFetcher = TarballFetcher(
        "crates",
        config.pm_config.source,
        config.exec_config.no_cache,
        config.exec_config.test,
    )
    if config.exec_config.fetch:
        files = fetcher.fetch()

    if not config.exec_config.no_cache:
        logger.log("Writing files to disk")
        fetcher.write(files)

    transformer = CratesTransformer(config)
    transformer.parse("crates")

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
