import os
from typing import Iterable, List, Type

from sqlalchemy import UUID, create_engine, func
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.decl_api import DeclarativeMeta
from sqlalchemy.orm.session import Session
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

    # TODO: the database client should not handle batching
    # we should move this to the transformer
    def _batch(
        self,
        items: Iterable[DeclarativeMeta],
        model: Type[DeclarativeMeta],
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> None:
        """just handles batching logic for any type of model we wanna insert"""
        self.logger.debug("starting a batch insert")
        with self.session() as session:
            batch = []
            for item in items:
                batch.append(item)
                if len(batch) == batch_size:
                    self.logger.debug(f"inserting {len(batch)} {model.__name__}")
                    self._insert_batch(batch, session, model)
                    batch = []  # reset

            if batch:
                self.logger.debug(f"finally, inserting {len(batch)} {model.__name__}")
                self._insert_batch(batch, session, model)

            session.commit()  # commit here

    def _insert_batch(
        self,
        batch: List[DeclarativeMeta],
        session: Session,
        model: Type[DeclarativeMeta],
    ) -> None:
        """
        inserts a batch of items, any model, into the database
        however, this mandates `on conflict do nothing`
        """
        # we use statements here, not the ORM because of the on_conflict_do_nothing
        # https://github.com/sqlalchemy/sqlalchemy/issues/5374
        stmt = (
            insert(model)
            .values([item.to_dict() for item in batch])
            .on_conflict_do_nothing()
        )
        session.execute(stmt)

    # TODO: inefficient insertion processes
    # the following functions are all about insertions
    # since we store `import_ids` in the db, we query the db for every single
    # insertion, which is not ideal
    # since we're removing ORMs completely, we should be able to batch
    # these queries as well

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

    def insert_versions(self, version_generator: Iterable[dict[str, str]]):
        def version_object_generator():
            for item in version_generator:
                crate_id = item["crate_id"]
                version = item["version"]
                import_id = item["import_id"]
                size = item["size"]
                published_at = item["published_at"]
                license_name = item["license"]
                downloads = item["downloads"]
                checksum = item["checksum"]

                # the crate_id is a Package.import_id
                package = self.select_package_by_import_id(crate_id)
                if package is None:
                    self.logger.warn(f"package with import_id {crate_id} not found")
                    continue
                package_id = package.id

                # similarly, for license_id
                license_id = self.select_license_by_name(license_name, create=True)

                yield Version(
                    package_id=package_id,
                    version=version,
                    import_id=import_id,
                    size=size,
                    published_at=published_at,
                    license_id=license_id,
                    downloads=downloads,
                    checksum=checksum,
                )

        self._batch(version_object_generator(), Version, DEFAULT_BATCH_SIZE)

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
        self, user_package_generator: Iterable[dict[str, str]], crates_sources_id: UUID
    ):
        def user_package_object_generator():
            for item in user_package_generator:
                crate_id = item["crate_id"]
                owner_id = item["owner_id"]

                user = self.select_crates_user_by_import_id(owner_id, crates_sources_id)
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
        self, user_version_generator: Iterable[dict[str, str]], crates_sources_id: UUID
    ):
        def user_version_object_generator():
            for item in user_version_generator:
                version_id = item["version_id"]
                user_id = item["published_by"]

                user = self.select_crates_user_by_import_id(user_id, crates_sources_id)
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
                yield URL(url=item)

        self._batch(url_object_generator(), URL, DEFAULT_BATCH_SIZE)

    def insert_package_urls(self, package_url_generator: Iterable[dict[str, str]]):
        def package_url_object_generator():
            for item in package_url_generator:
                yield PackageURL(
                    package_id=item["package_id"],
                    url_id=item["url_id"],
                    url_type_id=item["url_type_id"],
                )

        self._batch(package_url_object_generator(), PackageURL)

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

    # TODO: rename this to select_package_manager
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


if __name__ == "__main__":
    db = DB()

    # random tests
