from dataclasses import dataclass, field
from decimal import Decimal, getcontext
from typing import List, Tuple
from uuid import UUID

from sqlalchemy import func

from core.db import DB
from core.logger import Logger
from core.models import Canon, CanonPackage, Package, PackageManager, Source, URLType

getcontext().prec = 6

logger = Logger("graph.config")


class ConfigDB(DB):
    def __init__(self):
        super().__init__("graph.config::db")

    def get_homepage_url_type_id(self) -> UUID:
        with self.session() as session:
            result = (
                session.query(URLType.id).filter(URLType.name == "homepage").scalar()
            )
            if result is None:
                raise ValueError("homepage url type not found")
            return result

    def get_npm_pm_id(self) -> UUID:
        with self.session() as session:
            result = (
                session.query(PackageManager.id)
                .join(Source, PackageManager.source_id == Source.id)
                .filter(Source.type == "npm")
                .scalar()
            )
            if result is None:
                raise ValueError("npm package manager not found")
            return result

    def get_canons_with_source_types(
        self, source_types: List[str]
    ) -> List[Tuple[UUID, List[str]]]:
        with self.session() as session:
            return (
                session.query(
                    Canon.id, func.array_agg(Source.type).label("source_types")
                )
                .join(CanonPackage, Canon.id == CanonPackage.canon_id)
                .join(Package, CanonPackage.package_id == Package.id)
                .join(PackageManager, Package.package_manager_id == PackageManager.id)
                .join(Source, PackageManager.source_id == Source.id)
                .filter(Source.type.in_(source_types))
                .group_by(Canon.id)
                .all()
            )


db = ConfigDB()


class TeaRankConfig:
    alpha: Decimal = Decimal(0.85)
    favorites: dict[str, Decimal] = {
        "debian": Decimal(0.6),
        "homebrew": Decimal(0.4),
    }
    weights: dict[UUID, Decimal] = {}
    personalization: dict[UUID, Decimal] = {}

    def __init__(self) -> None:
        self.personalize()

    def personalize(self) -> None:
        """
        Personalize the weights for each canon based on the favorites.

        Suppose we have three packages: A, B, and C. Also suppose that:

        {
            "A": ["debian", "homebrew"],
            "B": ["debian"],
            "C": ["homebrew"],
        }

        The personalization vector is derived by solving for:

        1x + 0.6x + 0.4x = 1
        => x = 0.5

        So, A's weight is 0.5, B's weight 0.3, and C's weight 0.2.
        """
        # these are the system package managers
        source_types = list(self.favorites.keys())

        # get each canon, along with the list of package managers its part of
        canons_with_source_types = db.get_canons_with_source_types(source_types)

        # calculate raw weights for each canon based on favorites
        raw_weights = {}
        for canon_id, source_types in canons_with_source_types:
            # make source_types a set
            source_types = set(source_types)

            # sum the weights for all package managers this canon appears in
            weight = sum(self.favorites[source_type] for source_type in source_types)
            raw_weights[canon_id] = weight

        # normalize the weights so they sum to 1
        total_weight = sum(raw_weights.values())
        normalization_factor = (
            Decimal(1.0) / total_weight if total_weight > 0 else Decimal(0)
        )

        # Apply normalization and save to personalization map
        for canon_id, weight in raw_weights.items():
            self.personalization[canon_id] = weight * normalization_factor

    def __str__(self) -> str:
        return f"TeaRankConfig(alpha={self.alpha}, favorites={self.favorites}, weights={len(self.weights)}, personalization={len(self.personalization)})"  # noqa


class PMConfig:
    npm_pm_id: UUID = db.get_npm_pm_id()
    # TODO: we'll add PyPI, rubygems from when we load with legacy data

    def __str__(self) -> str:
        return f"PMConfig(npm_pm_id={self.npm_pm_id})"


class URLTypes:
    homepage_url_type_id: UUID = db.get_homepage_url_type_id()

    def __str__(self) -> str:
        return f"URLTypes(homepage_url_type_id={self.homepage_url_type_id})"


@dataclass
class Config:
    tearank_config: TeaRankConfig = field(default_factory=TeaRankConfig)
    pm_config: PMConfig = field(default_factory=PMConfig)
    url_types: URLTypes = field(default_factory=URLTypes)

    def __str__(self) -> str:
        return f"Config(tearank_config={self.tearank_config}, pm_config={self.pm_config}, url_types={self.url_types})"  # noqa


def load_config() -> Config:
    logger.debug("Loading config")
    return Config(
        tearank_config=TeaRankConfig(),
        pm_config=PMConfig(),
        url_types=URLTypes(),
    )
