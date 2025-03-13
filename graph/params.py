# TODO: probably a better way / config
from decimal import Decimal, getcontext

from core.logger import Logger
from core.models import Package
from graph.db import GraphDB

getcontext().prec = 6

logger = Logger("graph.params")


class PageRankParams:
    def __init__(self, db: GraphDB):
        self.db = db
        self.system_sources = ["debian", "homebrew"]
        self.alpha = Decimal(0.85)
        self.favorites = {
            "debian": Decimal(0.5),
            "homebrew": Decimal(0.5),
        }
        # TODO: we can enrich with this download data, for example
        self.weights: dict[Package, Decimal] = {}
        self.personalization: dict[Package, Decimal] = {}

    def personalize(self) -> None:
        canons_with_source_types = self.db.get_canons_with_source_types(
            self.system_sources
        )

        # Calculate raw weights for each canon based on package manager preferences
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

        logger.log(f"Personalization: {len(self.personalization)}")
