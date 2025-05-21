from dataclasses import dataclass
from typing import Dict, Set, Tuple
from uuid import UUID

from core.models import URL, LegacyDependency, Package, PackageURL


@dataclass
class CurrentGraph:
    package_map: Dict[str, Package]
    dependencies: Dict[UUID, Set[LegacyDependency]]


@dataclass
class URLKey:
    url: str
    url_type_id: UUID


@dataclass
class CurrentURLs:
    url_map: Dict[URLKey, URL]  # URL and URL Type ID to URL object
    package_urls: Dict[UUID, Set[PackageURL]]  # Package ID to PackageURL rows
