from enum import IntEnum
from dataclasses import dataclass
from sqlalchemy import UUID


class DependencyType(IntEnum):
    NORMAL = 0
    BUILD = 1  # used for build scripts
    DEV = 2  # used for testing or benchmarking
    OPTIONAL = 3

    def __str__(self):
        return self.name.lower()


@dataclass
class URLTypes:
    homepage: UUID
    repository: UUID
    documentation: UUID


@dataclass
class UserTypes:
    crates: UUID
    github: UUID
