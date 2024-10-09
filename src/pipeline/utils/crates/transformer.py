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
            "user_packages": "crate_owners.csv",
            "user_versions": "versions.csv",
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

    # gh_id is unique to github, and is from GitHub
    # our users table is unique on import_id and source_id
    # so, we actually get some github data for free here!
    def users(self):
        users_path = self.finder(self.files["users"])
        usernames = set()

        with open(users_path) as f:
            reader = csv.DictReader(f)
            for row in reader:
                gh_login = row["gh_login"]
                id = row["id"]

                # deduplicate
                if gh_login in usernames:
                    self.logger.warn(f"duplicate username: {id}, {gh_login}")
                    continue
                usernames.add(gh_login)

                # gh_login is a non-nullable column in crates, so we'll always be
                # able to load this
                source_id = self.user_types.github
                yield {"import_id": id, "username": gh_login, "source_id": source_id}

    # for crate_owners, owner_id and created_by are foreign keys on users.id
    # and owner_kind is 0 for user and 1 for team
    # secondly, created_at is nullable. we'll ignore for now and focus on owners
    def user_packages(self):
        user_packages_path = self.finder(self.files["user_packages"])

        with open(user_packages_path) as f:
            reader = csv.DictReader(f)
            for row in reader:
                owner_kind = int(row["owner_kind"])
                if owner_kind == 1:
                    continue

                crate_id = row["crate_id"]
                owner_id = row["owner_id"]

                yield {
                    "crate_id": crate_id,
                    "owner_id": owner_id,
                }

    # TODO: here, we are in the business of reopning files we've already opened before
    def user_versions(self):
        user_versions_path = self.finder(self.files["user_versions"])

        with open(user_versions_path) as f:
            reader = csv.DictReader(f)
            for row in reader:
                version_id = row["id"]
                published_by = row["published_by"]

                if published_by == "":
                    continue

                yield {"version_id": version_id, "published_by": published_by}

    # crates provides three urls for each crate: homepage, repository, and documentation
    # however, any of these could be null, so we should check for that
    # also, we're not going to deduplicate here
    def urls(self):
        urls_path = self.finder(self.files["urls"])

        with open(urls_path) as f:
            reader = csv.DictReader(f)
            for row in reader:
                homepage = row["homepage"]
                repository = row["repository"]
                documentation = row["documentation"]

                if homepage:
                    yield {"url": homepage, "url_type_id": self.url_types.homepage}

                if repository:
                    yield {"url": repository, "url_type_id": self.url_types.repository}

                if documentation:
                    yield {
                        "url": documentation,
                        "url_type_id": self.url_types.documentation,
                    }


if __name__ == "__main__":
    from src.pipeline.crates import initialize
    from src.pipeline.utils.pg import DB

    db = DB()
    config = initialize(db)
    x = CratesTransformer(config.url_types, config.user_types)

    db.insert_urls(x.urls())
