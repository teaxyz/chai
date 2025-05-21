import os
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple
from uuid import UUID, uuid4

from sqlalchemy import Insert, Update, create_engine, select, update
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session, sessionmaker

from core.logger import Logger
from core.models import (
    URL,
    BaseModel,
    DependsOnType,
    LoadHistory,
    Package,
    PackageManager,
    PackageURL,
    Source,
    URLType,
)

CHAI_DATABASE_URL = os.getenv("CHAI_DATABASE_URL")
DEFAULT_BATCH_SIZE = 10000


@dataclass
class CurrentURLs:
    url_map: Dict[Tuple[str, UUID], URL]  # URL and URL Type ID to URL object
    package_urls: Dict[UUID, Set[PackageURL]]  # Package ID to PackageURL rows


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

    def search_names(
        self, package_names: List[str], package_managers: List[UUID]
    ) -> List[str]:
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
            # and account for the fact that some
            return [
                name_to_url.get(name, None)
                for name in package_names
                if name in name_to_url
            ]

    # TODO: we should add the Cache class to the core structure, and have the individual
    # transfomers inherit from it. For now, keeping this as `Any`
    def load_urls(self, data: Dict[str, Any]) -> None:
        """
        Better way to load URLs by actually calculating the diff instead of relying on
        the db **not** to do something on a conflict

        - gets the current state of URLs in the database
        - checks the inputted data against the current state
        - identifies:
          - new URLs
          - new package-URL links
          - updates all package-URL links to right now (TODO: easiest optimization)
        """

        @dataclass
        class DiffResult:
            new_urls: List[URL]
            new_package_urls: List[PackageURL]
            urls_to_update: List[PackageURL]

        def get_desired_state() -> Dict[UUID, Set[URL]]:
            """Based on the cache, return the map of package ID to URLs"""
            desired_state: Dict[UUID, Set[URL]] = {}
            for cache in data.values():
                # first, a check
                if not hasattr(cache.package, "id") or cache.package.id is None:
                    self.logger.warn(
                        f"Package {cache.package.name} has no ID, skipping"
                    )
                    continue

                pkg_id = cache.package.id
                if pkg_id not in desired_state:
                    desired_state[pkg_id] = set()

                for url in cache.urls:
                    desired_state[pkg_id].add(url)

            return desired_state

        def diff(
            current_state: CurrentURLs, desired_state: Dict[UUID, Set[URL]]
        ) -> DiffResult:
            """
            Returns a DiffResult object with the new URLs, new package-URL links,
            and package-URL links to update.
            """
            # define all results as dictionaries, to prevent uniqueness constraints
            # from being violated
            new_urls: Dict[Tuple[str, UUID], URL] = {}
            new_package_urls: Dict[Tuple[UUID, UUID], PackageURL] = {}
            urls_to_update: Dict[Tuple[UUID, UUID], PackageURL] = {}

            for pkg_id, urls in desired_state.items():
                # what are the current URLs for this package?
                current_package_urls: Optional[Set[PackageURL]] = (
                    current_state.package_urls.get(pkg_id)
                )

                # let's make the current URLs a dictionary of URL ID to PackageURL
                # object, so that it's easy to figure out which PackageURL we need
                # to update later
                # create it now, so it could be empty if the package has no URLs
                # from the desired state loaded into the table
                current_urls: Dict[UUID, PackageURL] = {}

                if current_package_urls:
                    current_urls = {
                        current_package_url.url_id: current_package_url
                        for current_package_url in current_package_urls
                    }

                # what are the desired URLs for this package?
                for url in urls:
                    # does this url exist in current?
                    url_obj = current_state.url_map.get((url.url, url.url_type_id))

                    # if not:
                    if not url_obj:
                        # track as a new URL
                        if (url.url, url.url_type_id) not in new_urls:
                            new_urls[(url.url, url.url_type_id)] = URL(
                                id=uuid4(), url=url.url, url_type_id=url.url_type_id
                            )

                        # we'll use this ID to link the package to the URL
                        url_id = new_urls[(url.url, url.url_type_id)].id
                    else:
                        url_id = url_obj.id

                    # cool, so we have the ID now. we also know if we need to create it.
                    # now, let's do the diff to check if this URL is already linked to
                    # this package
                    if url_id not in current_urls:
                        if (pkg_id, url_id) not in new_package_urls:
                            new_package_url = PackageURL(
                                id=uuid4(),
                                package_id=pkg_id,
                                url_id=url_id,
                                created_at=self.now,
                                updated_at=self.now,
                            )
                            new_package_urls[(pkg_id, url_id)] = new_package_url
                    else:
                        # if it's already linked, just update the updated_at for now
                        # TODO: I think this we should have a latest tag in this table
                        # so we don't need to constantly ensure we're doing this update
                        to_update = current_urls[url_id]
                        to_update.updated_at = self.now
                        if (pkg_id, url_id) not in urls_to_update:
                            urls_to_update[(pkg_id, url_id)] = to_update

            result = DiffResult(
                new_urls=list(new_urls.values()),
                new_package_urls=list(new_package_urls.values()),
                urls_to_update=list(urls_to_update.values()),
            )
            return result

        #  first, get the desired state of all the URL relationships
        desired_state = get_desired_state()
        self.logger.debug(f"Length of desired state: {len(desired_state)}")

        # check if the URL strings from the above exist in the current state
        desired_urls = set(url.url for urls in desired_state.values() for url in urls)
        current_state = self.get_current_urls(desired_urls)

        # now, let's do the diff
        result = diff(current_state, desired_state)

        self.logger.debug(f"{len(result.new_urls)} new URLs")
        self.logger.debug(f"{len(result.new_package_urls)} new package-URL links")
        self.logger.debug(f"{len(result.urls_to_update)} package-URL links to update")

        with self.session() as session:
            try:
                # use batch insert
                self.logger.debug("Inserting new URLs")
                self.load(session, result.new_urls, pg_insert(URL))

                self.logger.debug("Inserting new package-URL links")
                self.load(session, result.new_package_urls, pg_insert(PackageURL))

                self.logger.debug("Updating package-URL links")
                if result.urls_to_update:
                    # values for batch updates needs to be explicitly specified on pkeys
                    # https://docs.sqlalchemy.org/en/20/orm/queryguide/dml.html#orm-queryguide-bulk-update
                    values = [
                        {"id": pkg_url.id, "updated_at": pkg_url.updated_at}
                        for pkg_url in result.urls_to_update
                    ]
                    stmt = update(PackageURL)
                    session.execute(stmt, values)

                session.commit()

            except Exception as e:
                self.logger.error("Error inserting URLs or PackageURLs")
                raise e

    def load(
        self, session: Session, data: List[BaseModel], stmt: Insert | Update
    ) -> None:
        """Smart batching utility"""
        if data:
            values: List[Dict[str, str | UUID | datetime]] = [
                obj.to_dict_v2() for obj in data
            ]
            self.batch(session, stmt, values, DEFAULT_BATCH_SIZE)

    def batch(
        self,
        session: Session,
        stmt: Insert | Update,
        values: List[Dict[str, str | UUID | datetime]],
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
                f"Processing batch {i//batch_size + 1}/{(len(values)-1)//batch_size + 1} ({len(batch)})"  # noqa
            )
            value_stmt = stmt.values(batch)
            session.execute(value_stmt)

    def get_current_urls(self, urls: Set[str]) -> CurrentURLs:
        stmt = (
            select(Package, PackageURL, URL)
            .select_from(URL)
            .join(PackageURL, PackageURL.url_id == URL.id, isouter=True)
            .join(Package, Package.id == PackageURL.package_id, isouter=True)
            .where(URL.url.in_(urls))
        )

        with self.session() as session:
            result = session.execute(stmt)

            url_map: Dict[Tuple[str, UUID], URL] = {}
            package_urls: Dict[UUID, Set[PackageURL]] = {}

            for pkg, pkg_url, url in result:
                url_map[(url.url, url.url_type_id)] = url

                # since it's a left join, we need to check if pkg is None
                if pkg is not None:
                    if pkg.id not in package_urls:
                        package_urls[pkg.id] = set()
                    package_urls[pkg.id].add(pkg_url)

            self.logger.debug(f"Length of url_map: {len(url_map)}")
            self.logger.debug(f"Length of package_urls: {len(package_urls)}")

            return CurrentURLs(url_map=url_map, package_urls=package_urls)


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
