from typing import Any, Dict, Iterable, List, Type

from sqlalchemy import UUID
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm.decl_api import DeclarativeMeta

from core.db import DB
from core.models import (
    URL,
    DependsOn,
    License,
    Package,
    PackageURL,
    User,
    UserPackage,
    UserVersion,
    Version,
)
from core.utils import build_query_params

DEFAULT_BATCH_SIZE = 10000


class CratesDB(DB):
    def __init__(self, logger_name: str):
        super().__init__(logger_name)

        # Initialize caches
        self.package_cache = {}
        self.user_cache = {}
        self.version_cache = {}
        self.license_cache = {}

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
            self.logger.debug(f"inserted {len(objects)} objects into {model.__name__}")
            session.commit()

    def insert_packages(
        self,
        package_generator: Iterable[str],
        package_manager_id: UUID,
        package_manager_name: str,
    ) -> List[UUID]:
        def process_package(item: Dict[str, str]):
            derived_id = f"{package_manager_name}/{item['name']}"
            return Package(
                derived_id=derived_id,
                name=item["name"],
                package_manager_id=package_manager_id,
                import_id=item["import_id"],
                readme=item["readme"],
            ).to_dict()

        batch = []
        for item in package_generator:
            batch.append(process_package(item))
            if len(batch) == DEFAULT_BATCH_SIZE:
                self._insert_batch(Package, batch)
                batch = []
        if batch:
            self._insert_batch(Package, batch)

    # TODO: needs explanation or simplification
    def _update_cache(
        self,
        cache: dict,
        model: Type[DeclarativeMeta],
        key_attr: str,
        value_attr: str,
        items: List[Dict[str, str]],
        query_param: str,
    ):
        ids_to_fetch = build_query_params(items, cache, query_param)
        if ids_to_fetch:
            fetched_objects = self._batch_fetch(model, key_attr, list(ids_to_fetch))
            cache.update(self._cache_objects(fetched_objects, key_attr, value_attr))

    def update_caches(
        self,
        items,
        update_packages=False,
        update_users=False,
        update_versions=False,
        update_licenses=False,
    ):
        if update_packages:
            self._update_cache(
                self.package_cache, Package, "import_id", "id", items, "crate_id"
            )
        if update_users:
            self._update_cache(
                self.user_cache, User, "import_id", "id", items, "owner_id"
            )
        if update_versions:
            self._update_cache(
                self.version_cache, Version, "import_id", "id", items, "version_id"
            )
        if update_licenses:
            self._update_cache(
                self.license_cache, License, "name", "id", items, "license"
            )

    def insert_versions(self, version_generator: Iterable[dict[str, str]]):
        batch = []
        for item in version_generator:
            batch.append(item)
            if len(batch) == DEFAULT_BATCH_SIZE:
                self.update_caches(batch, update_packages=True, update_licenses=True)
                versions = self._process_batch(batch, self._process_version)
                self._insert_batch(Version, versions)
                batch = []

        if batch:
            self.update_caches(batch, update_packages=True, update_licenses=True)
            versions = self._process_batch(batch, self._process_version)
            self._insert_batch(Version, versions)

    def _process_version(self, item: Dict[str, str]):
        package_id = self.package_cache.get(item["crate_id"])
        if not package_id:
            self.logger.warn(f"package {item['crate_id']} not found")
            return None

        license_id = self.license_cache.get(item["license"])
        if not license_id:
            self.logger.log(f"creating an entry for {item['license']}")
            license_id = self.select_license_by_name(item["license"], create=True)
            self.license_cache[item["license"]] = license_id

        if package_id is None or item["version"] is None or item["import_id"] is None:
            self.logger.warn(f"something weird: {item}")
            return None

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

    def insert_dependencies(self, dependency_generator: Iterable[dict[str, str]]):
        batch = []
        for item in dependency_generator:
            batch.append(item)
            if len(batch) == DEFAULT_BATCH_SIZE:
                self.update_caches(batch, update_versions=True, update_packages=True)
                dependencies = self._process_batch(batch, self._process_depends_on)
                self._insert_batch(DependsOn, dependencies)
                batch = []

        if batch:
            self.update_caches(batch, update_versions=True, update_packages=True)
            dependencies = self._process_batch(batch, self._process_depends_on)
            self._insert_batch(DependsOn, dependencies)

    def _process_depends_on(self, item: Dict[str, str]):
        return DependsOn(
            version_id=self.version_cache[item["version_id"]],
            dependency_id=self.package_cache[item["crate_id"]],
            semver_range=item["semver_range"],
        ).to_dict()

    def insert_users(self, user_generator: Iterable[dict[str, str]], source_id: UUID):
        def process_user(item: Dict[str, str]):
            return User(
                username=item["username"],
                import_id=item["import_id"],
                source_id=source_id,
            ).to_dict()

        batch = []
        for item in user_generator:
            batch.append(item)
            if len(batch) == DEFAULT_BATCH_SIZE:
                self._insert_batch(User, self._process_batch(batch, process_user))
                batch = []

        if batch:
            self._insert_batch(User, self._process_batch(batch, process_user))

    def insert_user_packages(self, user_package_generator: Iterable[dict[str, str]]):
        batch = []
        for item in user_package_generator:
            batch.append(item)
            if len(batch) == DEFAULT_BATCH_SIZE:
                self.update_caches(batch, update_packages=True, update_users=True)
                user_packages = self._process_batch(batch, self._process_user_package)
                self._insert_batch(UserPackage, user_packages)
                batch = []

        if batch:
            self.update_caches(batch, update_packages=True, update_users=True)
            user_packages = self._process_batch(batch, self._process_user_package)
            self._insert_batch(UserPackage, user_packages)

    def _process_user_package(self, item: Dict[str, str]):
        if item["owner_id"] not in self.user_cache:
            self.logger.warn(f"user {item['owner_id']} not found")
            return None

        if item["crate_id"] not in self.package_cache:
            self.logger.warn(f"package {item['crate_id']} not found")
            return None

        return UserPackage(
            user_id=self.user_cache[item["owner_id"]],
            package_id=self.package_cache[item["crate_id"]],
        ).to_dict()

    def insert_user_versions(
        self, user_version_generator: Iterable[dict[str, str]], source_id: UUID
    ):
        version_cache = {}
        user_cache = {}

        def fetch_versions_and_users(items: List[Dict[str, str]]):
            version_ids = build_query_params(items, version_cache, "version_id")
            user_ids = build_query_params(items, user_cache, "published_by")

            if version_ids:
                versions = self._batch_fetch(Version, "import_id", list(version_ids))
                version_cache.update(self._cache_objects(versions, "import_id", "id"))

            if user_ids:
                users = self._batch_fetch(User, "import_id", list(user_ids))
                user_cache.update(self._cache_objects(users, "import_id", "id"))

        def process_user_version(item: Dict[str, str]):
            user_id = user_cache.get(item["published_by"])
            if not user_id:
                self.logger.warn(f"user_id not found for {item['published_by']}")
                return None

            version_id = version_cache.get(item["version_id"])
            if not version_id:
                self.logger.warn(f"version_id not found for {item['version_id']}")
                return None

            return UserVersion(
                user_id=user_id,
                version_id=version_id,
            ).to_dict()

        batch = []
        for item in user_version_generator:
            batch.append(item)
            if len(batch) == DEFAULT_BATCH_SIZE:
                fetch_versions_and_users(batch)
                self._insert_batch(
                    UserVersion, self._process_batch(batch, process_user_version)
                )
                batch = []

        if batch:
            fetch_versions_and_users(batch)
            self._insert_batch(
                UserVersion, self._process_batch(batch, process_user_version)
            )

    def insert_urls(self, url_generator: Iterable[str]):
        def process_url(item: Dict[str, str]):
            return URL(url=item["url"], url_type_id=item["url_type_id"]).to_dict()

        batch = []
        for item in url_generator:
            batch.append(item)
            if len(batch) == DEFAULT_BATCH_SIZE:
                self._insert_batch(URL, self._process_batch(batch, process_url))
                batch = []

        if batch:
            self._insert_batch(URL, self._process_batch(batch, process_url))

    def insert_package_urls(self, package_url_generator: Iterable[dict[str, str]]):
        url_cache: Dict[tuple[str, str], UUID] = {}

        def fetch_packages_and_urls(items: List[Dict[str, str]]):
            package_ids = build_query_params(items, self.package_cache, "import_id")

            if package_ids:
                packages = self._batch_fetch(Package, "import_id", list(package_ids))
                self.package_cache.update(
                    self._cache_objects(packages, "import_id", "id")
                )

            # for url ids, we can't use batch_fetch, because we need to provide the
            # url_type_id in addition to the url string itself
            # so, let's do it the old fashioned way
            for item in items:
                url = item["url"]
                url_type_id = item["url_type_id"]
                if (url, url_type_id) not in url_cache:
                    url_cache[(url, url_type_id)] = self.select_url_by_url_and_type(
                        url, url_type_id
                    ).id

        def process_package_url(item: Dict[str, str]):
            package_id = self.package_cache.get(item["import_id"])
            if not package_id:
                self.logger.warn(f"package_id not found for {item['import_id']}")
                return None

            url_id = url_cache.get((item["url"], item["url_type_id"]))
            if not url_id:
                self.logger.warn(f"url_id not found for {item['url']}")
                return None

            return PackageURL(
                package_id=package_id,
                url_id=url_id,
            ).to_dict()

        batch = []
        for item in package_url_generator:
            batch.append(item)
            if len(batch) == DEFAULT_BATCH_SIZE:
                fetch_packages_and_urls(batch)
                self._insert_batch(
                    PackageURL, self._process_batch(batch, process_package_url)
                )
                batch = []

        if batch:
            fetch_packages_and_urls(batch)
            self._insert_batch(
                PackageURL, self._process_batch(batch, process_package_url)
            )

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

    def select_url_by_url_and_type(self, url: str, url_type_id: UUID) -> URL | None:
        with self.session() as session:
            result = (
                session.query(URL).filter_by(url=url, url_type_id=url_type_id).first()
            )
            if result:
                return result
