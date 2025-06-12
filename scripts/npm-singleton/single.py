#!/usr/bin/env pkgx +python@3.11 uv run --with requests==2.31.0 --with permalint==0.1.9
import argparse
import sys
from uuid import UUID, uuid4

import requests
from permalint import normalize_url

from core.config import Config, PackageManager
from core.db import DB
from core.models import URL, LegacyDependency, Package, PackageURL

NPM_API_URL = "https://registry.npmjs.org/{name}"


class ChaiDB(DB):
    def __init__(self):
        super().__init__("chai-singleton")

    def check_package_exists(self, derived_id: str) -> bool:
        with self.session() as session:
            return (
                session.query(Package).filter(Package.derived_id == derived_id).first()
                is not None
            )

    def get_package_by_derived_id(self, derived_id: str) -> Package:
        with self.session() as session:
            return (
                session.query(Package).filter(Package.derived_id == derived_id).first()
            )

    def load(
        self,
        pkg: Package,
        urls: list[URL],
        runtime_deps: list[LegacyDependency],
        dev_deps: list[LegacyDependency],
    ) -> None:
        """Load a package and its URLs into the database. Uses the same session to avoid
        transactional inconsistencies.

        Args:
            pkg: The package to load.
            urls: The URLs to load.
        """
        with self.session() as session:
            # Load the package first
            session.add(pkg)
            session.flush()  # to create the id
            pkg_id = pkg.id

            # Load the URLs
            for url in urls:
                session.add(url)
            session.flush()  # to create the id
            url_ids = [url.id for url in urls]

            # Create the package URL relationships
            for url_id in url_ids:
                session.add(PackageURL(package_id=pkg_id, url_id=url_id))

            # Create the legacy dependencies
            for dep in runtime_deps:
                session.add(dep)
            for dep in dev_deps:
                session.add(dep)
            session.commit()


def get_package_info(npm_package: str) -> tuple[bool, dict, str | None]:
    url = NPM_API_URL.format(name=npm_package)
    try:
        response = requests.get(url)
        if response.status_code != 200:
            return (
                False,
                {},
                f"Failed with status {response.status_code}: {response.text}",
            )
        return True, response.json(), None
    except Exception as e:
        return False, {}, f"Request failed: {e!s}"


def get_homepage(package_info: dict) -> tuple[bool, str | None]:
    try:
        return True, canonicalize(package_info["homepage"])
    except KeyError:
        return False, None
    except Exception as e:
        return False, str(e)


def get_repository_url(package_info: dict) -> tuple[bool, str | None]:
    try:
        return True, canonicalize(package_info["repository"]["url"])
    except KeyError:
        return False, None
    except Exception as e:
        return False, str(e)


def get_source_url(package_info: dict) -> tuple[bool, str | None]:
    try:
        repository_obj = package_info["repository"]
        if repository_obj["type"] == "git":
            return True, canonicalize(repository_obj["url"])
        else:
            return False, f"Repository is not a git URL: {repository_obj}"
    except KeyError:
        return False, None
    except Exception as e:
        return False, str(e)


def canonicalize(url: str) -> str:
    return normalize_url(url)


def get_latest_version(package_info: dict) -> tuple[bool, str | None]:
    try:
        dist_tags = package_info["dist-tags"]
        return True, dist_tags["latest"]
    except KeyError:
        return False, None


def get_version_info(package_info: dict, version: str) -> tuple[bool, dict | None]:
    try:
        return True, package_info["versions"][version]
    except KeyError:
        return False, None


def get_latest_version_dependencies(
    latest_version: dict,
) -> tuple[bool, dict[str, str]]:
    """Gets the dependencies from a version object from NPM's Registry API

    Returns:
      - a tuple of (success, dependencies) where dependencies is a dictionary
        keyed by dependency, with semver range as the value
    """
    try:
        deps = latest_version.get("dependencies", {})
        return True, deps
    except Exception:
        return False, {}


def get_latest_version_dev_dependencies(
    latest_version: dict,
) -> tuple[bool, dict[str, str]]:
    """Gets the development dependencies from a version object from NPM's Registry API

    Returns:
      - a tuple of (success, dependencies) where dependencies is a dictionary
        keyed by dependency, with semver range as the value
    """
    try:
        deps = latest_version.get("devDependencies", {})
        return True, deps
    except Exception:
        return False, {}


def check_dependencies_on_chai(
    db: ChaiDB, deps: dict[str, str]
) -> list[tuple[str, str, bool]]:
    """Check if dependencies exist on CHAI

    Args:
        db: ChaiDB instance
        deps: Dependencies to check

    Returns:
        List of tuples (dependency_name, semver_range, exists_on_chai)
    """
    results = []
    for dep_name, dep_range in deps.items():
        derived_id = f"npm/{dep_name}"
        exists = db.get_package_by_derived_id(derived_id) is not None
        results.append((dep_name, dep_range, exists))

    return results


def generate_url(url_type_id: UUID, url: str) -> URL:
    return URL(id=uuid4(), url=url, url_type_id=url_type_id)


def generate_legacy_dependencies(
    db: ChaiDB, pkg: Package, deps: dict[str, str], dependency_type_id: UUID
) -> tuple[list[LegacyDependency], list[tuple[str, str, bool]]]:
    legacy_deps: list[LegacyDependency] = []
    dep_status: list[tuple[str, str, bool]] = []

    for dep_name, dep_range in deps.items():
        derived_id = f"npm/{dep_name}"
        chai_dep: Package | None = db.get_package_by_derived_id(derived_id)
        exists = chai_dep is not None
        dep_status.append((dep_name, dep_range, exists))

        if not exists:
            continue

        dependency = LegacyDependency(
            package_id=pkg.id,
            dependency_id=chai_dep.id,
            dependency_type_id=dependency_type_id,
            semver_range=dep_range,
        )
        legacy_deps.append(dependency)

    return legacy_deps, dep_status


def print_status_report(
    package_name: str,
    exists_on_chai: bool,
    npm_response_ok: bool,
    npm_error: str | None,
    homepage_result: tuple[bool, str | None],
    repository_result: tuple[bool, str | None],
    source_result: tuple[bool, str | None],
    runtime_deps: list[tuple[str, str, bool]],
    dev_deps: list[tuple[str, str, bool]],
    changes_summary: dict[str, int] | None = None,
    dry_run: bool = False,
):
    """Print a formatted status report of the package processing"""
    divider = "-" * 45

    print(divider)
    print(f"Package: {package_name}")
    print(divider)

    if exists_on_chai:
        print(f"❌ Exiting bc {package_name} exists on CHAI")
    else:
        print(f"✅ {package_name} doesn't exist on CHAI")

    print(divider)

    if npm_response_ok:
        print("✅ OK from NPM")
    else:
        print(f"❌ Exiting bc response error from registry: {npm_error}")

    print(divider)

    homepage_ok, homepage = homepage_result
    if homepage_ok:
        print(f"✅ has homepage: {homepage}")
    else:
        print("❌ no homepage")

    repository_ok, repository = repository_result
    if repository_ok:
        print(f"✅ has repository: {repository}")
    else:
        print("❌ no repository")

    source_ok, source = source_result
    if source_ok:
        print(f"✅ has source: {source}")
    else:
        print("❌ no source")

    print(divider)
    print("Runtime Dependencies:")
    if not runtime_deps:
        print("(none)")
    else:
        for dep, semver, exists in runtime_deps:
            if exists:
                print(f"✅ {dep} / {semver} on CHAI")
            else:
                print(f"❌ {dep} / {semver} not on CHAI")

    print(divider)
    print("Dev Dependencies:")
    if not dev_deps:
        print("(none)")
    else:
        for dep, semver, exists in dev_deps:
            if exists:
                print(f"✅ {dep} / {semver} on CHAI")
            else:
                print(f"❌ {dep} / {semver} not on CHAI")

    print(divider)

    if changes_summary:
        if dry_run:
            print("DRY RUN: Would create the following rows:")
        else:
            print("Created the following rows:")

        for entity_type, count in changes_summary.items():
            print(f"  - {count} {entity_type}")
    else:
        print("Won't even create any rows")

    print(divider)


def process_package(package_name: str, dry_run: bool = False) -> bool:
    """Process a package and return True if successful, False otherwise"""
    config = Config(PackageManager.NPM)
    chai_db = ChaiDB()

    # Check if package exists
    derived_id = f"npm/{package_name}"
    exists_on_chai = chai_db.check_package_exists(derived_id)

    # Get Package Info from NPM
    npm_response_ok, package_info, npm_error = get_package_info(package_name)

    # Check URLs
    homepage_result = get_homepage(package_info) if npm_response_ok else (False, None)
    repository_result = (
        get_repository_url(package_info) if npm_response_ok else (False, None)
    )
    source_result = get_source_url(package_info) if npm_response_ok else (False, None)

    # Check latest version
    latest_version_result = (
        get_latest_version(package_info) if npm_response_ok else (False, None)
    )

    # Get version info
    version_info_result = (False, None)
    if npm_response_ok and latest_version_result[0]:
        version_info_result = get_version_info(package_info, latest_version_result[1])

    # Get dependencies
    runtime_deps_result = (False, {})
    dev_deps_result = (False, {})
    if npm_response_ok and version_info_result[0]:
        runtime_deps_result = get_latest_version_dependencies(version_info_result[1])
        dev_deps_result = get_latest_version_dev_dependencies(version_info_result[1])

    # Check dependencies on CHAI
    runtime_deps_status = check_dependencies_on_chai(chai_db, runtime_deps_result[1])
    dev_deps_status = check_dependencies_on_chai(chai_db, dev_deps_result[1])

    # Create entities to add to database if not in dry run mode and all checks pass
    changes_summary = {
        "Package": 1,
        "URLs": 0,
        "PackageURLs": 0,
        "Runtime Dependencies": 0,
        "Dev Dependencies": 0,
    }

    # Early exit if necessary conditions aren't met
    if exists_on_chai or not npm_response_ok:
        print_status_report(
            package_name,
            exists_on_chai,
            npm_response_ok,
            npm_error,
            homepage_result,
            repository_result,
            source_result,
            runtime_deps_status,
            dev_deps_status,
            None,
            dry_run,
        )
        return False

    # Create Package
    derived_id = f"npm/{package_name}"
    package_manager_id = config.pm_config.pm_id
    import_id = f"npm-singleton/{package_name}"
    readme = package_info.get("readme", "")

    pkg = Package(
        id=uuid4(),
        name=package_name,
        derived_id=derived_id,
        package_manager_id=package_manager_id,
        import_id=import_id,
        readme=readme,
    )

    # URLs
    urls = []
    if homepage_result[0]:
        urls.append(
            generate_url(config.url_types.homepage, normalize_url(homepage_result[1]))
        )
    if repository_result[0]:
        urls.append(
            generate_url(
                config.url_types.repository, normalize_url(repository_result[1])
            )
        )
    if source_result[0]:
        urls.append(
            generate_url(config.url_types.source, normalize_url(source_result[1]))
        )

    changes_summary["URLs"] = len(urls)
    changes_summary["PackageURLs"] = len(urls)

    # Dependencies
    runtime_deps, _ = generate_legacy_dependencies(
        chai_db, pkg, runtime_deps_result[1], config.dependency_types.runtime
    )
    dev_deps, _ = generate_legacy_dependencies(
        chai_db, pkg, dev_deps_result[1], config.dependency_types.development
    )

    changes_summary["Runtime Dependencies"] = len(runtime_deps)
    changes_summary["Dev Dependencies"] = len(dev_deps)

    # Print status report
    print_status_report(
        package_name,
        exists_on_chai,
        npm_response_ok,
        npm_error,
        homepage_result,
        repository_result,
        source_result,
        runtime_deps_status,
        dev_deps_status,
        changes_summary,
        dry_run,
    )

    # Load the package into the database (unless in dry run mode)
    if not dry_run:
        chai_db.load(pkg, urls, runtime_deps, dev_deps)
        print("✅ Successfully committed changes to database")
    else:
        print("🌵 Dry run: No changes committed to database")

    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Load a single NPM package by name into CHAI"
    )
    parser.add_argument("name", help="Name of the NPM package")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Check package without committing to database",
    )
    args = parser.parse_args()

    success = process_package(args.name, args.dry_run)
    if not success:
        sys.exit(1)
