from dataclasses import dataclass

from sqlalchemy import UUID


@dataclass
class URLTypes:
    homepage: UUID
    repository: UUID
    documentation: UUID


@dataclass
class UserTypes:
    crates: UUID
    github: UUID
