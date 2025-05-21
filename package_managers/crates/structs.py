from dataclasses import dataclass
from enum import IntEnum
from typing import List


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
class Crate:
    id: int
    name: str
    readme: str
    latest_version: str
    homepage: str
    repository: str
    documentation: str
    latest_version_downloads: int
    latest_version_dependencies: List[CrateDependency]
    # TODO; add the user fields here
