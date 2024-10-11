import os
from typing import Any, Dict, Iterable, List, Type

from sqlalchemy import UUID, create_engine, func
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.decl_api import DeclarativeMeta
from src.pipeline.models import (
    DependsOn,
    License,
    LoadHistory,
    Package,
    PackageManager,
    PackageURL,
    Source,
    URLType,
    User,
    URL,
    UserPackage,
    UserVersion,
    Version,
)
from src.pipeline.utils.logger import Logger

CHAI_DATABASE_URL = os.getenv("CHAI_DATABASE_URL")
DEFAULT_BATCH_SIZE = 10000


# ORMs suck, go back to SQL
class DB:
    def __init__(self):
        self.logger = Logger("DB")
        self.engine = create_engine(CHAI_DATABASE_URL)
        self.session = sessionmaker(self.engine)
        self.logger.debug("connected")

    def _cache_objects(
        self, objects: List[DeclarativeMeta], key_attr: str, value_attr: str
    ):
        """cache an object based on a key and value attribute"""
        return {getattr(obj, key_attr): getattr(obj, value_attr) for obj in objects}

    def _batch_fetch(self, model: Type[DeclarativeMeta], attr: str, values: List[Any]):
        """fetch a batch of objects based on a list of values for a given attribute"""
        with self.session() as session:
            return session.query(model).filter(getattr(model, attr).in_(values)).all()

    def _process_batch(
        self, items: List[Dict[str, Any]], process_func: callable
    ) -> List[DeclarativeMeta]:
        """process a batch of items, and filter out any Nones"""
        return [obj for obj in (process_func(item) for item in items) if obj]

    def _insert_batch(
        self,
        model: Type[DeclarativeMeta],
        objects: List[Dict[str, Any]],
    ) -> None:
        """
        inserts a batch of items, any model, into the database
        however, this mandates `on conflict do nothing`
        """
        # we use statements here, not the ORM because of the on_conflict_do_nothing
        # https://github.com/sqlalchemy/sqlalchemy/issues/5374
        with self.session() as session:
            stmt = insert(model).values(objects).on_conflict_do_nothing()
            session.execute(stmt)
            session.commit()

    def insert_packages(
        self,
        package_generator: Iterable[str],
        package_manager_id: UUID,
        package_manager_name: str,
    ) -> List[UUID]:
        def package_object_generator():
            for item in package_generator:
                name = item["name"]
                import_id = item["import_id"]
                readme = item["readme"]
                derived_id = f"{package_manager_name}/{name}"

                yield Package(
                    derived_id=derived_id,
                    name=name,
                    package_manager_id=package_manager_id,
                    import_id=import_id,
                    readme=readme,
                )

        return self._batch(package_object_generator(), Package, DEFAULT_BATCH_SIZE)

    def select_packages_by_import_ids(self, iids: Iterable[str]) -> List[Package]:
        with self.session() as session:
            return session.query(Package).filter(Package.import_id.in_(iids)).all()

    def select_licenses_by_name(self, names: Iterable[str]) -> List[License]:
        with self.session() as session:
            return session.query(License).filter(License.name.in_(names)).all()

    def insert_versions(self, version_generator: Iterable[dict[str, str]]):
        package_cache = {}
        license_cache = {}

        # TODO: utils
        def build_query_params(
            items: List[Dict[str, str]], cache: dict, attr: str
        ) -> List[str]:
            params = set()
            for item in items:
                if item[attr] not in cache:
                    params.add(item[attr])
            return list(params)

        # this function updates our cache of import_ids to packages and
        # names to licenses
        def fetch_packages_and_licenses(items: List[Dict[str, str]]):
            # build the query params
            crate_ids = build_query_params(items, package_cache, "crate_id")
            license_names = build_query_params(items, license_cache, "license")

            # fetch the packages and licenses
            if crate_ids:
                packages = self._batch_fetch(Package, "import_id", list(crate_ids))
                package_cache.update(self._cache_objects(packages, "import_id", "id"))

            if license_names:
                licenses = self.select_licenses_by_name(license_names)
                license_cache.update(self._cache_objects(licenses, "name", "id"))

        def process_version(item: Dict[str, str]):
            package_id = package_cache[item["crate_id"]]
            license_id = license_cache[item["license"]]

            return Version(
                package_id=package_id,
                version=item["version"],
                import_id=item["import_id"],
                size=item["size"],
                published_at=item["published_at"],
                license_id=license_id,
                downloads=item["downloads"],
                checksum=item["checksum"],
            ).to_dict()

        batch = []
        for item in version_generator:
            batch.append(item)
            if len(batch) == DEFAULT_BATCH_SIZE:
                # update the caches
                fetch_packages_and_licenses(batch)
                versions = self._process_batch(batch, process_version)
                self._insert_batch(Version, versions)
                batch = []  # reset

    def insert_dependencies(self, dependency_generator: Iterable[dict[str, str]]):
        def dependency_object_generator():
            for item in dependency_generator:
                start_id = item["start_id"]
                end_id = item["end_id"]
                semver_range = item["semver_range"]
                _ = item["dependency_type"]  # dependency_type

                version = self.select_version_by_import_id(start_id)
                if version is None:
                    self.logger.warn(f"version with import_id {start_id} not found")
                    continue
                version_id = version.id

                dependency = self.select_package_by_import_id(end_id)
                if dependency is None:
                    self.logger.warn(f"package with import_id {end_id} not found")
                    continue
                dependency_id = dependency.id

                now = func.now()

                yield DependsOn(
                    version_id=version_id,
                    dependency_id=dependency_id,
                    semver_range=semver_range,
                    # TODO: db should do this
                    created_at=now,
                    updated_at=now,
                    # dependency_type=dependency_type, TODO: add this
                )

        self._batch(dependency_object_generator(), DependsOn, DEFAULT_BATCH_SIZE)

    def insert_load_history(self, package_manager_id: str):
        with self.session() as session:
            session.add(LoadHistory(package_manager_id=package_manager_id))
            session.commit()

    def insert_url_types(self, name: str) -> URLType:
        with self.session() as session:
            session.add(URLType(name=name))
            session.commit()
            return session.query(URLType).filter_by(name=name).first()

    def insert_users(self, user_generator: Iterable[dict[str, str]]):
        def user_object_generator():
            for item in user_generator:
                username = item["username"]
                import_id = item["import_id"]
                source_id = item["source_id"]

                yield User(username=username, import_id=import_id, source_id=source_id)

        self._batch(user_object_generator(), User, DEFAULT_BATCH_SIZE)

    def insert_user_packages(
        self, user_package_generator: Iterable[dict[str, str]], source_id: UUID
    ):
        def user_package_object_generator():
            for item in user_package_generator:
                crate_id = item["crate_id"]
                owner_id = item["owner_id"]

                user = self.select_crates_user_by_import_id(owner_id, source_id)
                if user is None:
                    self.logger.warn(f"user with import_id {owner_id} not found")
                    continue

                package = self.select_package_by_import_id(crate_id)
                if package is None:
                    self.logger.warn(f"package with import_id {crate_id} not found")
                    continue

                yield UserPackage(user_id=user.id, package_id=package.id)

        self._batch(user_package_object_generator(), UserPackage, DEFAULT_BATCH_SIZE)

    def insert_user_versions(
        self, user_version_generator: Iterable[dict[str, str]], source_id: UUID
    ):
        def user_version_object_generator():
            for item in user_version_generator:
                version_id = item["version_id"]
                user_id = item["published_by"]

                user = self.select_crates_user_by_import_id(user_id, source_id)
                if user is None:
                    self.logger.warn(f"user with import_id {user_id} not found")
                    continue

                version = self.select_version_by_import_id(version_id)
                if version is None:
                    self.logger.warn(f"version with import_id {version_id} not found")
                    continue

                yield UserVersion(user_id=user.id, version_id=version.id)

        self._batch(user_version_object_generator(), UserVersion, DEFAULT_BATCH_SIZE)

    def insert_urls(self, url_generator: Iterable[str]):
        def url_object_generator():
            for item in url_generator:
                url = item["url"]
                url_type_id = item["url_type_id"]
                yield URL(url=url, url_type_id=url_type_id)

        self._batch(url_object_generator(), URL, DEFAULT_BATCH_SIZE)

    def insert_package_urls(self, package_url_generator: Iterable[dict[str, str]]):
        def package_url_object_generator():
            for item in package_url_generator:
                package_import_id = item["import_id"]
                url = item["url"]
                url_type_id = item["url_type_id"]

                self.logger.debug(f"looking for package {package_import_id}")
                package = self.select_package_by_import_id(package_import_id)
                if package is None:
                    self.logger.warn(
                        f"package with import_id {package_import_id} not found"
                    )
                    continue
                package_id = package.id

                url = self.select_url_by_url_and_type(url, url_type_id)
                if url is None:
                    self.logger.warn(f"url {url} not found")
                    continue
                url_id = url.id

                yield PackageURL(
                    package_id=package_id,
                    url_id=url_id,
                )

        self._batch(package_url_object_generator(), PackageURL, DEFAULT_BATCH_SIZE / 10)

    def insert_source(self, name: str) -> UUID:
        with self.session() as session:
            session.add(Source(type=name))
            session.commit()
            return session.query(Source).filter_by(type=name).first()

    def insert_package_manager(self, source_id: UUID) -> PackageManager:
        with self.session() as session:
            session.add(PackageManager(source_id=source_id))
            session.commit()
            return session.query(PackageManager).filter_by(source_id=source_id).first()

    def print_statement(self, stmt):
        dialect = postgresql.dialect()
        compiled_stmt = stmt.compile(
            dialect=dialect, compile_kwargs={"literal_binds": True}
        )
        self.logger.log(str(compiled_stmt))

    def select_url_type(self, url_type: str, create: bool = False) -> URLType:
        with self.session() as session:
            result = session.query(URLType).filter_by(name=url_type).first()
            if result:
                return result
            if create:
                return self.insert_url_types(url_type)
            return None

    def select_url_types_homepage(self, create: bool = False) -> URLType | None:
        return self.select_url_type("homepage", create)

    def select_url_types_repository(self, create: bool = False) -> URLType | None:
        return self.select_url_type("repository", create)

    def select_url_types_documentation(self, create: bool = False) -> URLType | None:
        return self.select_url_type("documentation", create)

    def select_package_manager_by_name(
        self, package_manager: str, create: bool = False
    ) -> PackageManager | None:
        with self.session() as session:
            # get the package manager
            result = (
                session.query(PackageManager)
                .join(Source, PackageManager.source_id == Source.id)
                .filter(Source.type == package_manager)
                .first()
            )

            # return it if it exists
            if result:
                return result

            if create:
                result = self.insert_source(package_manager)
                id = result.id
                return self.insert_package_manager(id)

            return None

    def select_package_by_import_id(self, import_id: str) -> Package | None:
        with self.session() as session:
            result = session.query(Package).filter_by(import_id=import_id).first()
            if result:
                return result

    def select_license_by_name(
        self, license_name: str, create: bool = False
    ) -> UUID | None:
        with self.session() as session:
            result = session.query(License).filter_by(name=license_name).first()
            if result:
                return result.id
            if create:
                session.add(License(name=license_name))
                session.commit()
                return session.query(License).filter_by(name=license_name).first().id
            return None

    def select_version_by_import_id(self, import_id: str) -> Version | None:
        with self.session() as session:
            result = session.query(Version).filter_by(import_id=import_id).first()
            if result:
                return result

    def select_package_manager_name_by_id(self, id: UUID) -> str | None:
        with self.session() as session:
            result = (
                session.query(Source.type)
                .join(PackageManager, PackageManager.source_id == Source.id)
                .filter(PackageManager.id == id)
                .first()
            )
            if result:
                return result.type

    def select_source_by_name(self, name: str, create: bool = False) -> Source | None:
        with self.session() as session:
            result = session.query(Source).filter_by(type=name).first()
            if result:
                return result
            if create:
                return self.insert_source(name)
            return None

    def select_crates_user_by_import_id(
        self, import_id: str, crates_sources_id: UUID
    ) -> User | None:
        with self.session() as session:
            result = (
                session.query(User)
                .filter_by(import_id=import_id, source_id=crates_sources_id)
                .first()
            )
            if result:
                return result

    def select_url_by_url_and_type(self, url: str, url_type_id: UUID) -> URL | None:
        with self.session() as session:
            result = (
                session.query(URL).filter_by(url=url, url_type_id=url_type_id).first()
            )
            if result:
                return result
