from dataclasses import dataclass
from typing import Dict, List, Optional, Set, Tuple
from uuid import UUID

from core.models import LegacyDependency, Package, PackageURL


@dataclass
class Cache:
    package_cache: Dict[str, Package]
    url_cache: Dict[Tuple[str, UUID], UUID]
    package_url_cache: Dict[UUID, Set[PackageURL]]
    dependency_cache: Dict[UUID, Set[LegacyDependency]]


@dataclass
class Actual:
    formula: str
    description: str
    license: str
    homepage: str
    source: str
    repository: Optional[str]
    build_dependencies: Optional[List[str]]
    dependencies: Optional[List[str]]
    test_dependencies: Optional[List[str]]
    recommended_dependencies: Optional[List[str]]
    optional_dependencies: Optional[List[str]]
