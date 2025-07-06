import csv
from collections.abc import Generator

from core.config import Config
from core.transformer import Transformer
from core.utils import is_github_url
from package_managers.crates.structs import (
    Crate,
    CrateDependency,
    CrateLatestVersion,
    CrateUser,
    DependencyType,
)


class CratesTransformer(Transformer):
    def __init__(self, config: Config):
        super().__init__("crates")
        self.config = config
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
                yield from reader
        except KeyError as exc:
            raise KeyError(
                f"Missing {file_name} from self.files: {self.files}"
            ) from exc
        except FileNotFoundError as exc:
            self.logger.error(f"Missing {file_path} from data directory")
            raise FileNotFoundError(f"Missing {file_path} file") from exc
        except Exception as e:
            self.logger.error(f"Error reading {file_path}: {e}")
            raise e

    def parse(self) -> None:
        # first go through crates.csv to
        # here, we can get the import_id, name, homepage, documentation, repository
        # and also source, from repo if it is like GitHub
        for row in self._open_csv("crates"):
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

        self.logger.log(f"Parsed {len(self.crates)} crates")

        # populate the map of crate_id to latest_version_id & all latest_version_ids
        latest_versions: set[int]
        latest_versions_map: dict[int, int]
        latest_versions, latest_versions_map = self._load_latest_versions()
        self.logger.log(f"Loaded {len(latest_versions)} latest versions")

        # also build the map of user_id to CrateUser object
        users: dict[int, CrateUser] = self._load_users()
        self.logger.log(f"Loaded {len(users)} users")

        # now, iterate through the versions.csv, and populate LatestVersion objects,
        # only if the version_id is in the latest_versions set
        for row in self._open_csv("versions"):
            version_id = int(row["id"])
            crate_id = int(row["crate_id"])

            # ignore if this version is not the latest
            if version_id not in latest_versions:
                continue

            checksum = row["checksum"]
            downloads = int(row["downloads"])
            license = row["license"]
            num = row["num"]
            published_at = row["created_at"]

            # make a CrateUser object from the published_by
            published_by = row["published_by"]
            published_by_user: CrateUser | None = (
                users[int(published_by)] if published_by else None
            )

            latest_version = CrateLatestVersion(
                version_id,
                checksum,
                downloads,
                license,
                num,
                published_at,
                published_by_user,
            )

            # map this LatestVersion to the crate in self.crates
            self.crates[crate_id].latest_version = latest_version

        self.logger.log("Parsed the latest versions for each crate")

        # finally, parse through the dependencies.csv
        # again, we only care about the dependencies for the latest version
        for row in self._open_csv("dependencies"):
            start_id = int(row["version_id"])

            # ignore if this version is not the latest
            if start_id not in latest_versions:
                continue

            # map both ids to crates
            end_crate_id = int(row["crate_id"])
            start_crate_id = int(latest_versions_map[start_id])

            # guard
            if start_crate_id not in self.crates:
                raise ValueError(f"Crate {start_crate_id} not found in self.crates")

            kind = int(row["kind"])

            # guard
            if kind not in [0, 1, 2]:
                raise ValueError(f"Unknown dependency kind: {kind}")

            dependency_type = DependencyType(kind)
            semver = row["req"]

            dependency = CrateDependency(
                start_crate_id, end_crate_id, dependency_type, semver
            )

            # add this dependency to the crate
            self.crates[start_crate_id].latest_version.dependencies.append(dependency)

        self.logger.log("Parsed the dependencies for each crate")

    def _load_latest_versions(self) -> tuple[set[int], dict[int, int]]:
        latest_versions: set[int] = set()
        latest_versions_map: dict[int, int] = {}
        for row in self._open_csv("latest_versions"):
            crate_id = int(row["crate_id"])
            version_id = int(row["version_id"])
            latest_versions.add(version_id)
            latest_versions_map[version_id] = crate_id

        return latest_versions, latest_versions_map

    def _load_users(self) -> dict[int, CrateUser]:
        users: dict[int, CrateUser] = {}
        for row in self._open_csv("users"):
            user_id = int(row["id"])
            name = row["name"]
            github_username = row["gh_login"]
            user = CrateUser(user_id, name, github_username)
            users[user_id] = user

        return users
