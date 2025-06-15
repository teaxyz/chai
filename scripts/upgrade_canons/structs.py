from dataclasses import dataclass
from datetime import datetime
from uuid import UUID


# let's make classes defining the data models, since scripts can't really access ./core
@dataclass
class URL:
    id: UUID
    url: str
    url_type_id: UUID
    created_at: datetime
    updated_at: datetime


@dataclass
class PackageURL:
    id: UUID
    package_id: UUID
    url_id: UUID
    created_at: datetime
    updated_at: datetime
