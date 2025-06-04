from uuid import UUID

from core.config import Config, PackageManager
from core.fetcher import TarballFetcher
from core.logger import Logger
from core.models import (
    URL,
    LegacyDependency,
    Package,
    PackageURL,
)
from core.structs import Cache, URLKey
from package_managers.crates.db import CratesDB
from package_managers.crates.diff import Diff
from package_managers.crates.transformer import CratesTransformer


def identify_deletions(transformer: CratesTransformer, db: CratesDB) -> set[int]:
    """
    Identifies crates that are in the db but not in the transformer

    Cargo enables deletion of crates from the registry, if:
      - the crate has been published for less than 72 hours
      - the crate only has a single owner
      - the crate has been downloaded less than 500 times for each month it has been
      - the crate is not depended upon by any other crate on crates.io

    The risk is that the namespace for an invalid import_id is now available, and
    might be taken by a new crate, which would violate our uniqueness constraint on
    derived_id

    Returns:
      - a set of import_ids that are in the db but not in the transformer

    References:
      - https://crates.io/policies
      - https://rurust.github.io/cargo-docs-ru/policies.html
    """
    logger = Logger("crates_identify_deletions")

    # db needs to know the cargo id to chai id
    cargo_id_to_chai_id: dict[str, UUID] = db.get_cargo_id_to_chai_id()

    transformer_import_ids: set[int] = {int(c.id) for c in transformer.crates.values()}
    db_import_ids: set[int] = {int(p) for p in cargo_id_to_chai_id.keys()}

    # calculate deletions
    deletions: set[int] = db_import_ids - transformer_import_ids
    if deletions:
        logger.warn(
            f"There are {len(deletions)} crates in the db but not in the registry"
        )

    return deletions


def main(config: Config, db: CratesDB):
    logger = Logger("crates_main")
    logger.log("Starting crates_main")

    # fetch the files from cargo
    if config.exec_config.fetch:
        fetcher: TarballFetcher = TarballFetcher(
            "crates",
            str(config.pm_config.source),
            config.exec_config.no_cache,
            config.exec_config.test,
        )
        files = fetcher.fetch()
        logger.log(f"Fetched {len(files)} files")

    # write the files to disk
    if not config.exec_config.no_cache:
        fetcher.write(files)
        logger.log("Wrote files to disk")

    # transform the files into a list of crates
    transformer = CratesTransformer(config)
    transformer.parse()
    logger.log(f"Parsed {len(transformer.crates)} crates")

    # identify crates we need to delete from CHAI because they are no longer on
    # cargo
    deletions = identify_deletions(transformer, db)
    logger.log(f"Identified {len(deletions)} crates to delete")
    if deletions:
        db.delete_packages_by_import_id(deletions)
        logger.log(f"Deleted {len(deletions)} crates")

    # the transformer object has transformer.crates, which has all the info
    # now, let's build the db's cache
    # we need the graph object from the db
    db.set_current_graph()
    logger.log("Set current graph")

    # we need URLs
    crates_urls: set[str] = set()
    for crate in transformer.crates.values():
        crates_urls.add(crate.homepage)
        crates_urls.add(crate.repository)
        crates_urls.add(crate.documentation)
    db.set_current_urls(crates_urls)
    logger.log("Set current URLs")

    # now, we can build the cache
    cache = Cache(
        db.graph.package_map,
        db.urls.url_map,
        db.urls.package_urls,
        db.graph.dependencies,
    )
    logger.log("Built cache")

    # now, we can do the diff
    new_packages: list[Package] = []
    updated_packages: list[dict] = []
    new_urls: dict[URLKey, URL] = {}
    new_package_urls: list[PackageURL] = []
    updated_package_urls: list[dict] = []
    new_deps: list[LegacyDependency] = []
    removed_deps: list[LegacyDependency] = []

    diff = Diff(config, cache)
    for pkg in transformer.crates.values():
        pkg_id, pkg_obj, update_payload = diff.diff_pkg(pkg)
        if pkg_obj:
            new_packages.append(pkg_obj)
        if update_payload:
            updated_packages.append(update_payload)

        # URLs
        resolved_urls = diff.diff_url(pkg, new_urls)

        # package URLs
        new_links, updated_links = diff.diff_pkg_url(pkg_id, resolved_urls)
        if new_links:
            new_package_urls.extend(new_links)
        if updated_links:
            updated_package_urls.extend(updated_links)

        # finally, dependencies
        new_dependencies, removed_dependencies = diff.diff_deps(pkg)
        if new_dependencies:
            new_deps.extend(new_dependencies)
        if removed_dependencies:
            removed_deps.extend(removed_dependencies)

    logger.log(f"Diffed {len(transformer.crates)} crates!")

    # make new_urls a list of new URLs
    final_new_urls = list(new_urls.values())

    db.ingest(
        new_packages,
        final_new_urls,
        new_package_urls,
        new_deps,
        removed_deps,
        updated_packages,
        updated_package_urls,
    )

    logger.log("✅ Done")


if __name__ == "__main__":
    config = Config(PackageManager.CRATES)
    db = CratesDB(config)
    main(config, db)
