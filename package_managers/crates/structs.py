from dataclasses import dataclass, field
from datetime import datetime
from enum import IntEnum


class DependencyType(IntEnum):
    NORMAL = 0
    BUILD = 1  # used for build scripts
    DEV = 2  # used for testing or benchmarking
    OPTIONAL = 3

    def __str__(self):
        return self.name.lower()


@dataclass
class CrateDependency:
    crate_id: int
    dependency_id: int
    dependency_type: DependencyType  # kind
    semver_range: str  # req


@dataclass
class CrateUser:
    # from users.csv or teams.csv
    id: int
    name: str | None = None
    github_username: str | None = None


@dataclass
class CrateLatestVersion:
    # latest version ID is from default_versions.csv
    # data is from versions.csv
    id: int
    checksum: str
    downloads: int
    license: str
    num: str
    published_by: CrateUser
    published_at: datetime  # version.created_at
    # dependencies.csv
    dependencies: list[CrateDependency] = field(default_factory=list)


@dataclass
class Crate:
    # from crates.csv
    id: int
    name: str
    readme: str
    homepage: str
    repository: str
    documentation: str
    source: str | None = None
    # from versions.csv
    latest_version: CrateLatestVersion | None = None
