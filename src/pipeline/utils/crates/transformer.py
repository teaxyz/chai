import csv
from typing import Dict, Generator

from src.pipeline.utils.utils import safe_int
from src.pipeline.utils.crates.structures import DependencyType, URLTypes, UserTypes
from src.pipeline.utils.transformer import Transformer


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
        }
        self.url_types = url_types
        self.user_types = user_types

    def packages(self) -> Generator[str, None, None]:
        projects_path = self.finder(self.files["projects"])

        with open(projects_path) as f:
            reader = csv.DictReader(f)
            for row in reader:
                crate_id = row["id"]
                name = row["name"]
                readme = row["readme"]

                yield {"name": name, "import_id": crate_id, "readme": readme}

    def versions(self) -> Generator[Dict[str, int], None, None]:
        versions_path = self.finder(self.files["versions"])

        with open(versions_path) as f:
            reader = csv.DictReader(f)
            for row in reader:
                crate_id = row["crate_id"]
                version_num = row["num"]
                version_id = row["id"]
                crate_size = safe_int(row["crate_size"])
                created_at = row["created_at"]
                license = row["license"]
                downloads = safe_int(row["downloads"])
                checksum = row["checksum"]

                # TODO: published_by is there

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

    def dependencies(self):
        dependencies_path = self.finder(self.files["dependencies"])

        with open(dependencies_path) as f:
            reader = csv.DictReader(f)
            for row in reader:
                start_id = row["version_id"]
                end_id = row["crate_id"]
                req = row["req"]
                kind = int(row["kind"])

                # map string to enum
                dependency_type = DependencyType(kind)

                yield {
                    "start_id": start_id,
                    "end_id": end_id,
                    "semver_range": req,
                    "dependency_type": dependency_type,
                }

    def users(self):
        users_path = self.finder(self.files["users"])

        with open(users_path) as f:
            reader = csv.DictReader(f)
            for row in reader:
                gh_id = row["gh_id"]
                gh_login = row["gh_login"]
                id = row["id"]
                name = row["name"]

                # yield two rows, one for crates and one for GitHub
                source_id = self.user_types.crates
                yield {"import_id": id, "username": name, "source_id": source_id}

                source_id = self.user_types.github
                yield {"import_id": gh_id, "username": gh_login, "source_id": source_id}
