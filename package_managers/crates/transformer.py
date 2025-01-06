import csv
from typing import Dict, Generator

from core.config import URLTypes, UserTypes
from core.transformer import Transformer
from core.utils import safe_int
from package_managers.crates.structs import DependencyType


# crates provides homepage and repository urls, so we'll initialize this transformer
# with the ids for those url types
class CratesTransformer(Transformer):
    def __init__(self, url_types: URLTypes, user_types: UserTypes):
        super().__init__("crates")
        self.files = {
            "projects": "crates.csv",
            "versions": "versions.csv",
            "dependencies": "dependencies.csv",
            "users": "users.csv",
            "urls": "crates.csv",
            "user_packages": "crate_owners.csv",
            "user_versions": "versions.csv",
        }
        self.url_types = url_types
        self.user_types = user_types

    def _read_csv_rows(self, file_key: str) -> Generator[Dict[str, str], None, None]:
        """
        Helper method to read rows from a CSV file based on the file key.
        
        Args:
            file_key (str): The key corresponding to the desired CSV file in self.files.
        
        Yields:
            Dict[str, str]: A dictionary representing a row in the CSV file.
        """
        file_path = self.finder(self.files[file_key])
        try:
            with open(file_path, newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    yield row
        except FileNotFoundError:
            self.logger.error(f"File not found: {file_path}")
        except Exception as e:
            self.logger.error(f"Error reading {file_path}: {e}")

    def packages(self) -> Generator[Dict[str, str], None, None]:
        for row in self._read_csv_rows("projects"):
            crate_id = row["id"]
            name = row["name"]
            readme = row["readme"]

            yield {"name": name, "import_id": crate_id, "readme": readme}

    def versions(self) -> Generator[Dict[str, str], None, None]:
        for row in self._read_csv_rows("versions"):
            crate_id = row["crate_id"]
            version_num = row["num"]
            version_id = row["id"]
            crate_size = safe_int(row["crate_size"])
            created_at = row["created_at"]
            license = row["license"]
            downloads = safe_int(row["downloads"])
            checksum = row["checksum"]

            yield {
                "crate_id": crate_id,
                "version": version_num,
                "import_id": version_id,
                "size": crate_size,
                "published_at": created_at,
                "license": license,
                "downloads": downloads,
                "checksum": checksum,
            }

    def dependencies(self) -> Generator[Dict[str, str], None, None]:
        for row in self._read_csv_rows("dependencies"):
            start_id = row["version_id"]
            end_id = row["crate_id"]
            req = row["req"]
            kind = int(row["kind"])


            try:
                # map string to enum
                dependency_type = DependencyType(kind)
            except ValueError:
                self.logger.warning(f"Unknown dependency kind: {kind}")
                continue

            yield {
                "version_id": start_id,
                "crate_id": end_id,
                "semver_range": req,
                "dependency_type": dependency_type,
            }

    # gh_id is unique to GitHub, and is from GitHub
    # our users table is unique on import_id and source_id
    # so, we actually get some GitHub data for free here!
    def users(self) -> Generator[Dict[str, str], None, None]:
        usernames = set()
        for row in self._read_csv_rows("users"):
            gh_login = row["gh_login"]
            user_id = row["id"]

            # Deduplicate based on gh_login
            if gh_login in usernames:
                self.logger.warning(f"Duplicate username detected: ID={user_id}, Username={gh_login}")
                continue
            usernames.add(gh_login)

            # gh_login is a non-nullable column in crates, so we'll always be
            # able to load this
            source_id = self.user_types.github
            yield {"import_id": user_id, "username": gh_login, "source_id": source_id}

    # for crate_owners, owner_id and created_by are foreign keys on users.id
    # and owner_kind is 0 for user and 1 for team
    # secondly, created_at is nullable. we'll ignore for now and focus on owners
    def user_packages(self) -> Generator[Dict[str, str], None, None]:
        for row in self._read_csv_rows("user_packages"):
            owner_kind = int(row["owner_kind"])
            if owner_kind == 1:
                continue  # Skip if owner is a team

            crate_id = row["crate_id"]
            owner_id = row["owner_id"]

            yield {
                "crate_id": crate_id,
                "owner_id": owner_id,
            }

    # TODO: reopening files: versions.csv contains all the published_by ids
    def user_versions(self) -> Generator[Dict[str, str], None, None]:
        for row in self._read_csv_rows("user_versions"):
            version_id = row["id"]
            published_by = row["published_by"]

            if published_by == "":
                continue

            yield {"version_id": version_id, "published_by": published_by}

    # crates provides three urls for each crate: homepage, repository, and documentation
    # however, any of these could be null, so we should check for that
    # also, we're not going to deduplicate here
    def urls(self) -> Generator[Dict[str, str], None, None]:
        for row in self._read_csv_rows("urls"):
            homepage = row.get("homepage", "").strip()
            repository = row.get("repository", "").strip()
            documentation = row.get("documentation", "").strip()

            if homepage:
                yield {"url": homepage, "url_type_id": self.url_types.homepage}

            if repository:
                yield {"url": repository, "url_type_id": self.url_types.repository}

            if documentation:
                yield {
                    "url": documentation,
                    "url_type_id": self.url_types.documentation,
                }

    # TODO: reopening files: crates.csv contains all the urls
    def package_urls(self) -> Generator[Dict[str, str], None, None]:
        for row in self._read_csv_rows("urls"):
            crate_id = row["id"]
            homepage = row.get("homepage", "").strip()
            repository = row.get("repository", "").strip()
            documentation = row.get("documentation", "").strip()

            if homepage:
                yield {
                    "import_id": crate_id,
                    "url": homepage,
                    "url_type_id": self.url_types.homepage,
                }

            if repository:
                yield {
                    "import_id": crate_id,
                    "url": repository,
                    "url_type_id": self.url_types.repository,
                }

            if documentation:
                yield {
                    "import_id": crate_id,
                    "url": documentation,
                    "url_type_id": self.url_types.documentation,
                }
