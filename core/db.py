import os

from sqlalchemy import create_engine
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import sessionmaker

from core.logger import Logger
from core.models import (
    DependsOnType,
    LoadHistory,
    PackageManager,
    Source,
    URLType,
)

CHAI_DATABASE_URL = os.getenv("CHAI_DATABASE_URL")
DEFAULT_BATCH_SIZE = 10000


class DB:
    def __init__(self, logger_name: str):
        self.logger = Logger(logger_name)
        self.engine = create_engine(CHAI_DATABASE_URL)
        self.session = sessionmaker(self.engine)
        self.logger.debug("connected")

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
