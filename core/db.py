import os
from collections import defaultdict
from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import Insert, Result, Update, create_engine, select, update
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import Session, sessionmaker

from core.logger import Logger
from core.models import (
    URL,
    BaseModel,
    DependsOnType,
    LegacyDependency,
    LoadHistory,
    Package,
    PackageManager,
    PackageURL,
    Source,
    URLType,
)
from core.structs import CurrentGraph, CurrentURLs, URLKey

CHAI_DATABASE_URL = os.getenv("CHAI_DATABASE_URL")
DEFAULT_BATCH_SIZE = 10000


class DB:
    def __init__(self, logger_name: str):
        self.logger = Logger(logger_name)
        self.engine = create_engine(CHAI_DATABASE_URL)
        self.session = sessionmaker(self.engine)
        self.logger.debug("connected")
        self.now: datetime = datetime.now()

    def insert_load_history(self, package_manager_id: str):
        with self.session() as session:
            session.add(LoadHistory(package_manager_id=package_manager_id))
            session.commit()

    def print_statement(self, stmt):
        dialect = postgresql.dialect()
        compiled_stmt = stmt.compile(
            dialect=dialect, compile_kwargs={"literal_binds": True}
        )
        self.logger.log(str(compiled_stmt))

    def close(self):
        self.logger.debug("closing")
        self.engine.dispose()

    def search_names(
        self, package_names: list[str], package_managers: list[UUID]
    ) -> list[str]:
        """Return Homepage URLs for packages with these names"""

        with self.session() as session:
            results = (
                session.query(Package, URL)
                .join(PackageURL, PackageURL.package_id == Package.id)
                .join(URL, PackageURL.url_id == URL.id)
                .join(URLType, URL.url_type_id == URLType.id)
                .filter(URLType.name == "homepage")
                .filter(Package.name.in_(package_names))
                .filter(Package.package_manager_id.in_(package_managers))
                .all()
            )

            # build a mapping
            name_to_url = {result.Package.name: result.URL.url for result in results}

            # return in the order preserved by the input (bc its relevant)
            # and account for the fact that some names might not have a URL
            return [
                name_to_url.get(name) for name in package_names if name in name_to_url
            ]

    def current_graph(self, package_manager_id: UUID) -> CurrentGraph:
        """Get the packages and dependencies for a specific package manager"""
        package_map: dict[str, Package] = defaultdict(Package)
        dependencies: dict[UUID, set[LegacyDependency]] = defaultdict(set)

        stmt = (
            select(Package, LegacyDependency)
            .select_from(Package)
            .join(
                LegacyDependency,
                onclause=Package.id == LegacyDependency.package_id,
                isouter=True,
            )
            .where(Package.package_manager_id == package_manager_id)
        )

        with self.session() as session:
            result: Result[tuple[Package, LegacyDependency]] = session.execute(stmt)

            for pkg, dep in result:
                # add to the package map, by import_id, which is usually name
                package_map[pkg.import_id] = pkg

                # and add to the dependencies map as well
                if dep:  # check because it's an outer join, so might be None
                    dependencies[pkg.id].add(dep)

        self.logger.debug(f"Cached {len(package_map)} packages")

        return CurrentGraph(package_map, dependencies)

    def _build_current_urls(
        self, result: Result[tuple[Package, PackageURL, URL]]
    ) -> CurrentURLs:
        """Build the CurrentURLs result based on a query of Package, PackageURL, URL"""
        url_map: dict[URLKey, URL] = {}
        package_urls: dict[UUID, set[PackageURL]] = {}

        for pkg, pkg_url, url in result:
            url_key = URLKey(url.url, url.url_type_id)
            url_map[url_key] = url

            # since it's a left join, we need to check if pkg is None
            if pkg is not None:
                if pkg.id not in package_urls:
                    package_urls[pkg.id] = set()
                package_urls[pkg.id].add(pkg_url)

        self.logger.debug(f"Cached {len(url_map)} URLs")
        self.logger.debug(f"Cached {len(package_urls)} package URLs")

        return CurrentURLs(url_map=url_map, package_urls=package_urls)

    def current_urls(self, urls: set[str]) -> CurrentURLs:
        """Get the Package URL Relationships for a given set of URLs"""
        stmt = (
            select(Package, PackageURL, URL)
            .select_from(URL)
            .join(PackageURL, PackageURL.url_id == URL.id, isouter=True)
            .join(Package, Package.id == PackageURL.package_id, isouter=True)
            .where(URL.url.in_(urls))
        )

        with self.session() as session:
            result: Result[tuple[Package, PackageURL, URL]] = session.execute(stmt)
            return self._build_current_urls(result)

    def all_current_urls(self) -> CurrentURLs:
        """Get all the URLs and the Packages they are tied to from CHAI"""
        stmt = (
            select(Package, PackageURL, URL)
            .select_from(URL)
            .join(PackageURL, PackageURL.url_id == URL.id, isouter=True)
            .join(Package, Package.id == PackageURL.package_id, isouter=True)
        )
        with self.session() as session:
            result: Result[tuple[Package, PackageURL, URL]] = session.execute(stmt)
            return self._build_current_urls(result)

    def load(
        self, session: Session, data: list[BaseModel], stmt: Insert | Update
    ) -> None:
        """Smart batching utility"""
        if data:
            values: list[dict[str, str | UUID | datetime]] = [
                obj.to_dict_v2() for obj in data
            ]
            self.batch(session, stmt, values, DEFAULT_BATCH_SIZE)

    def batch(
        self,
        session: Session,
        stmt: Insert | Update,
        values: list[dict[str, str | UUID | datetime]],
        batch_size: int = DEFAULT_BATCH_SIZE,
    ):
        """
        Utility to batch insert or update, but doesn't commit!

        Inputs:
        - session: the sessionmaker object, so create it before you use it
        - stmt: the insert or update statement to construct, without the values
        - values: the values to insert or update - generally using to_dict_v2()
        - batch_size: the batch size, defaults to 10000
        - obj_name: the name of the object being inserted for logging
        """
        for i in range(0, len(values), batch_size):
            batch = values[i : i + batch_size]
            self.logger.log(
                f"Processing batch {i // batch_size + 1}/{(len(values) - 1) // batch_size + 1} ({len(batch)})"
            )
            value_stmt = stmt.values(batch)
            session.execute(value_stmt)

    def ingest(
        self,
        new_packages: list[Package],
        new_urls: list[URL],
        new_package_urls: list[PackageURL],
        new_deps: list[LegacyDependency],
        removed_deps: list[LegacyDependency],
        updated_packages: list[dict[str, UUID | str | datetime]],
        updated_package_urls: list[dict[str, UUID | datetime]],
    ) -> None:
        """
        Ingests a list of new, updated, and deleted objects from the database.

        It flushes after each insert, to ensure that the database is in a valid
        state prior to the next batch of ingestions.

        TODO: if pkey is set in the values provided, then sqlalchemy will use
        psycopg2.executemany(...), which is quicker, but still the slowest of all
        execution options provided by psycopg2. The best one is execute_values, which
        is **only** available for inserts, and can be used as follows:

        looks like sqlalchemy^2 has a native support for insertmanyvalues, but
        **I think** we need to pass the data in as a list[dict] instead of objects.
        See: https://docs.sqlalchemy.org/en/20/core/connections.html#engine-insertmanyvalues


        Inputs:
        - new_packages: a list of new Package objects
        - new_urls: a list of new URL objects
        - new_package_urls: a list of new PackageURL objects
        - updated_packages: a list of updated Package objects
        - updated_package_urls: a list of updated PackageURL objects
        - new_deps: a list of new LegacyDependency objects
        - removed_deps: a list of removed LegacyDependency objects
        """
        self.logger.log("-" * 100)
        self.logger.log("Going to load")
        self.logger.log(f"New packages: {len(new_packages)}")
        self.logger.log(f"New URLs: {len(new_urls)}")
        self.logger.log(f"New package URLs: {len(new_package_urls)}")
        self.logger.log(f"Updated packages: {len(updated_packages)}")
        self.logger.log(f"Updated package URLs: {len(updated_package_urls)}")
        self.logger.log(f"New dependencies: {len(new_deps)}")
        self.logger.log(f"Removed dependencies: {len(removed_deps)}")
        self.logger.log("-" * 100)

        with self.session() as session:
            try:
                # 1. Add all new objects with granular flushes
                self.execute(session, new_packages, "add", "new packages")
                self.execute(session, new_urls, "add", "new urls")
                self.execute(session, new_package_urls, "add", "new package urls")
                self.execute(session, removed_deps, "delete", "removed dependencies")
                self.execute(session, new_deps, "add", "new dependencies")

                # 2. Perform updates (these will now operate on a flushed state)
                if updated_packages:
                    session.execute(update(Package), updated_packages)

                if updated_package_urls:
                    session.execute(update(PackageURL), updated_package_urls)

                # 3. Commit all changes
                session.commit()
                self.logger.log("✅ Successfully ingested")
            except Exception as e:
                self.logger.error(f"Error during batched ingest: {e}")
                session.rollback()
                raise e

    def execute(self, session: Session, data: list[Any], method: str, log: str) -> None:
        if method not in ["add", "delete"]:
            raise ValueError(f"db.execute({method}) is unknown")

        if data:
            match method:
                case "add":
                    session.add_all(data)
                case "delete":
                    self.remove_all(session, data)
                case _:
                    pass

            session.flush()
        self.logger.log(f"✅ {len(data):,} {log}")

    def remove_all(self, session: Session, data: list[Any]) -> None:
        for item in data:
            session.delete(item)


class ConfigDB(DB):
    def __init__(self):
        super().__init__("ConfigDB")

    def select_package_manager_by_name(self, package_manager: str) -> PackageManager:
        with self.session() as session:
            result = (
                session.query(PackageManager)
                .join(Source, PackageManager.source_id == Source.id)
                .filter(Source.type == package_manager)
                .first()
            )

            if result:
                return result

            raise ValueError(f"Package manager {package_manager} not found")

    def select_url_types_by_name(self, name: str) -> URLType:
        with self.session() as session:
            return session.query(URLType).filter(URLType.name == name).first()

    def select_source_by_name(self, name: str) -> Source:
        with self.session() as session:
            return session.query(Source).filter(Source.type == name).first()

    def select_dependency_type_by_name(self, name: str) -> DependsOnType:
        with self.session() as session:
            return (
                session.query(DependsOnType).filter(DependsOnType.name == name).first()
            )


if __name__ == "__main__":
    db = ConfigDB()
    print(db.search_names(["elfutils.org", "elfutils"]))
