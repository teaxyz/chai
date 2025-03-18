#!/usr/bin/env pkgx +python@3.11 uv run
from dataclasses import dataclass, field
from decimal import Decimal, getcontext
from typing import List, Tuple
from uuid import UUID

from sqlalchemy import func

from core.db import DB
from core.logger import Logger
from core.models import Canon, CanonPackage, Package, PackageManager, Source, URLType

logger = Logger("graph.config")
SYSTEM_PACKAGE_MANAGERS = ["homebrew", "debian"]

# setup decimal
getcontext().prec = 9
getcontext().rounding = "ROUND_HALF_UP"


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
        return self.get_pm_id_by_name("npm")[0][0]

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

    def get_pm_id_by_name(self, name: str | List[str]) -> UUID:
        if isinstance(name, str):
            name = [name]

        with self.session() as session:
            result = (
                session.query(PackageManager.id)
                .join(Source, PackageManager.source_id == Source.id)
                .filter(Source.type.in_(name))
                .all()
            )
            if result is None:
                raise ValueError(f"package manager {name} not found")
            return result


db = ConfigDB()


class TeaRankConfig:
    alpha: Decimal = Decimal(0.85)
    favorites: dict[str, Decimal] = {}
    weights: dict[UUID, Decimal] = {}
    personalization: dict[UUID, Decimal] = {}
    split_ratio: Decimal = Decimal(0.8)
    tol: Decimal = Decimal(1e-6)
    max_iter: int = 1000000

    def map_favorites(self, package_managers: List[str]) -> None:
        for pm in package_managers:
            match pm:
                case "homebrew":
                    self.favorites[pm] = Decimal(0.4)
                case "debian":
                    self.favorites[pm] = Decimal(0.6)
                case _:
                    raise ValueError(f"Unknown system package manager: {pm}")

    def __init__(self) -> None:
        self.map_favorites(SYSTEM_PACKAGE_MANAGERS)
        self.personalize()

    def personalize(self) -> None:
        """Adjust canon weights proportionally to the sum of `favorites` in their
        associated package managers, normalized to total 1."""

        def coefficient(source_types: List[str]) -> Decimal:
            return sum(self.favorites[source_type] for source_type in source_types)

        # these are the system package managers
        source_types: List[str] = list(self.favorites.keys())

        # get each canon, along with the list of package managers its part of
        canons_with_source_types: List[Tuple[UUID, List[str]]] = (
            db.get_canons_with_source_types(source_types)
        )
        logger.debug(f"Queried system_pm canons: {len(canons_with_source_types)}")

        # calculate raw weights for each canon based on favorites
        raw_weights = {}
        total = Decimal(0)
        for canon_id, source_types in canons_with_source_types:
            # make source_types a set to deduplicate
            source_types = set(source_types)

            # sum the weights for all package managers this canon appears in
            weight = coefficient(source_types)
            raw_weights[canon_id] = weight
            total += weight

        constant = Decimal(1) / total

        for canon_id, weight in raw_weights.items():
            self.personalization[canon_id] = weight * constant

        logger.debug(f"Personalized {len(self.personalization)} canons")

    def __str__(self) -> str:
        return f"TeaRankConfig(alpha={self.alpha}, favorites={self.favorites}, weights={len(self.weights)}, personalization={len(self.personalization)})"  # noqa


class PMConfig:
    npm_pm_id: UUID = db.get_npm_pm_id()
    system_pm_ids: List[UUID] = [
        id[0] for id in db.get_pm_id_by_name(SYSTEM_PACKAGE_MANAGERS)
    ]
    # TODO: we'll add PyPI, rubygems from when we load with legacy data

    def __str__(self) -> str:
        return (
            f"PMConfig(npm_pm_id={self.npm_pm_id}, system_pm_ids={self.system_pm_ids})"
        )


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


if __name__ == "__main__":
    logger.mode = Logger.VERBOSE
    config = load_config()
    logger.debug(config)
