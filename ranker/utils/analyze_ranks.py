#!/usr/bin/env pkgx +python@3.11 uv run --with pandas --with sqlalchemy

"""Script to analyze rank data and generate formatted CSV output.

Usage:
    python analyze_ranks.py [--file PATH_TO_RANK_FILE]
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, distinct, func, select
from sqlalchemy.orm import Session

from core.models import (
    Canon,
    CanonPackage,
    Package,
    PackageManager,
    Source,
)


def get_latest_rank_file() -> Path:
    """Get the path to the latest rank file."""
    data_dir = Path("data/ranker/ranks")
    latest_symlink = data_dir / "latest.json"
    return latest_symlink.resolve()


def get_rank_file(filename: str | None = None) -> Path:
    """Get the path to the rank file.

    Args:
        filename: Optional path to a specific rank file.

    Returns:
        Path to the rank file.

    Raises:
        FileNotFoundError: If the specified file doesn't exist.
    """
    if filename:
        file_path = Path(filename)
        if not file_path.exists():
            raise FileNotFoundError(f"Rank file not found: {filename}")
        return file_path

    return get_latest_rank_file()


def load_rank_data(file_path: Path) -> dict[str, float]:
    """Load rank data from JSON file."""
    with open(file_path) as f:
        return json.load(f)


def get_output_filename(input_path: Path) -> Path:
    """Generate output filename based on input filename."""
    # Extract the rank number from filenames like "ranks_37_0.7"
    parts = input_path.stem.split("_")
    rank_num = "_".join(parts[1:]) if len(parts) >= 2 else input_path.stem

    output_dir = Path("data/ranker/analysis")
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir / f"formatted_ranks_{rank_num}.csv"


def get_package_data(ranks: dict[str, float], db_session: Session) -> pd.DataFrame:
    """Query database for package information and combine with ranks."""
    # Query for package data including URLs and aggregated package info
    query = (
        select(
            Canon.id.label("canon_id"),
            Canon.url.label("homepage_url"),
            Canon.name.label("package_name"),
            func.array_agg(distinct(Source.type)).label("package_managers"),
            func.array_agg(distinct(Package.name)).label("package_names"),
        )
        .join(CanonPackage, Canon.id == CanonPackage.canon_id)
        .join(Package, CanonPackage.package_id == Package.id)
        .join(PackageManager, Package.package_manager_id == PackageManager.id)
        .join(Source, PackageManager.source_id == Source.id)
        .group_by(Canon.id, Canon.url, Canon.name)
    )

    results = pd.DataFrame(db_session.execute(query))

    # Convert UUID objects to strings in results DataFrame
    results["canon_id"] = results["canon_id"].astype(str)

    # Convert ranks to DataFrame and merge
    ranks_df = pd.DataFrame.from_dict(ranks, orient="index", columns=["tea_rank"])
    ranks_df.index.name = "canon_id"
    ranks_df.reset_index(inplace=True)

    # Merge and sort
    final_df = pd.merge(ranks_df, results, on="canon_id")
    if final_df.empty:
        raise ValueError(
            "No data to process - no matching canon_ids between ranks and database results"
        )

    final_df.sort_values(["tea_rank"], ascending=[False], inplace=True)
    return final_df[
        [
            "canon_id",
            "package_name",
            "tea_rank",
            "homepage_url",
            "package_managers",
            "package_names",
        ]
    ]


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Analyze rank data and generate formatted CSV output"
    )
    parser.add_argument(
        "--file",
        type=str,
        default=None,
        help="Path to a specific rank file. If not provided, the latest rank file will be used.",
    )
    return parser.parse_args()


def main() -> None:
    """Main function to process rank data and generate CSV."""
    # Parse command-line arguments
    args = parse_args()

    # Setup database connection
    engine = create_engine(os.environ["CHAI_DATABASE_URL"])

    # Get input and output paths
    rank_file = get_rank_file(args.file)
    output_file = get_output_filename(rank_file)
    print(f"Output will be saved to: {output_file}")

    # Process data
    ranks = load_rank_data(rank_file)
    with Session(engine) as session:
        result_df = get_package_data(ranks, session)

    # Save output
    result_df.to_csv(output_file, index=False)


if __name__ == "__main__":
    main()
