#! /usr/bin/env pkgx +python@3.11 uv run

from datetime import datetime
from uuid import UUID

from core.config import Config, PackageManager
from core.logger import Logger
from core.models import URL, LegacyDependency, Package, PackageURL
from core.structs import Cache, URLKey
from package_managers.homebrew.db import HomebrewDB
from package_managers.homebrew.diff import Diff
from package_managers.homebrew.formulae import HomebrewFetcher


def main(config: Config, db: HomebrewDB) -> None:
    """A diff-based attempt at loading into CHAI"""

    logger = Logger("homebrew_main")
    fetcher = HomebrewFetcher(config)
    brew = fetcher.fetch()

    # get the URLs & set that
    brew_urls = {b.source for b in brew} | {b.homepage for b in brew}
    db.set_current_urls(brew_urls)
    logger.log("Set current URLs")

    # get the caches here
    cache = Cache(
        db.graph.package_map,
        db.urls.url_map,
        db.urls.package_urls,
        db.graph.dependencies,
    )

    # total set of updates we'll make are:
    new_packages: list[Package] = []
    new_urls: dict[URLKey, URL] = {}  # we'll convert this later
    new_package_urls: list[PackageURL] = []
    updated_packages: list[dict[str, UUID | str | datetime]] = []
    updated_package_urls: list[dict[str, UUID | datetime]] = []
    new_deps: list[LegacyDependency] = []
    removed_deps: list[LegacyDependency] = []

    diff = Diff(config, cache)
    for i, pkg in enumerate(brew):
        pkg_id, pkg_obj, update_payload = diff.diff_pkg(pkg)
        if pkg_obj:
            logger.debug(f"New package: {pkg_obj.name}")
            new_packages.append(pkg_obj)
        if update_payload:
            logger.debug(f"Updated package: {update_payload['id']}")
            updated_packages.append(update_payload)

        # NOTE: resolved urls is a map of url types to final URL ID for this pkg
        # also, &new_urls gets passed in AND mutated
        resolved_urls = diff.diff_url(pkg, new_urls)

        # now, new package urls
        new_links, updated_links = diff.diff_pkg_url(pkg_id, resolved_urls)
        if new_links:
            logger.debug(f"New package URLs: {len(new_links)}")
            new_package_urls.extend(new_links)
        if updated_links:
            logger.debug(f"Updated package URLs: {len(updated_links)}")
            updated_package_urls.extend(updated_links)

        # finally, dependencies
        new_dependencies, removed_dependencies = diff.diff_deps(pkg)
        if new_dependencies:
            logger.debug(f"New dependencies: {len(new_dependencies)}")
            new_deps.extend(new_dependencies)
        if removed_dependencies:
            logger.debug(f"Removed dependencies: {len(removed_dependencies)}")
            removed_deps.extend(removed_dependencies)

        if config.exec_config.test and i > 100:
            break

    # final cleanup is to replace the new_urls map with a list
    final_new_urls = list(new_urls.values())

    # send to loader
    db.ingest(
        new_packages,
        final_new_urls,
        new_package_urls,
        updated_packages,
        updated_package_urls,
        new_deps,
        removed_deps,
    )


if __name__ == "__main__":
    config = Config(PackageManager.HOMEBREW)
    db = HomebrewDB("homebrew_db_main", config)
    main(config, db)
