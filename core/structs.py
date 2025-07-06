from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

from core.models import URL, LegacyDependency, Package, PackageURL


@dataclass
class CurrentGraph:
    package_map: dict[str, Package]
    dependencies: dict[UUID, set[LegacyDependency]]


@dataclass(frozen=True)
class URLKey:
    url: str
    url_type_id: UUID


@dataclass
class CurrentURLs:
    url_map: dict[URLKey, URL]  # URL and URL Type ID to URL object
    package_urls: dict[UUID, set[PackageURL]]  # Package ID to PackageURL rows


@dataclass
class Cache:
    package_map: dict[str, Package]
    url_map: dict[URLKey, URL]
    package_urls: dict[UUID, set[PackageURL]]
    dependencies: dict[UUID, set[LegacyDependency]]


@dataclass
class DiffResult:
    new_packages: list[Package]
    new_urls: dict[URLKey, URL]
    new_package_urls: list[PackageURL]
    updated_packages: list[dict[str, UUID | str | datetime]]
    updated_package_urls: list[dict[str, UUID | datetime]]
    new_deps: list[LegacyDependency]
    removed_deps: list[LegacyDependency]
