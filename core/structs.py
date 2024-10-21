from dataclasses import dataclass
from enum import Enum
from typing import Dict

from sqlalchemy import UUID


class PackageManager(Enum):
    CRATES = "crates"
    HOMEBREW = "homebrew"


PackageManagerIDs = Dict[PackageManager, UUID]
Sources = Dict[PackageManager, str]


@dataclass
class URLTypes:
    homepage: UUID
    repository: UUID
    documentation: UUID
    source: UUID


@dataclass
class UserTypes:
    crates: UUID
    github: UUID


@dataclass
class DependencyTypes:
    build: UUID
    development: UUID
    runtime: UUID
    test: UUID
    optional: UUID
    recommended: UUID
