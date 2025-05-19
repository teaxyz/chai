"""
Common test fixtures and configurations.
"""

import uuid
from unittest.mock import MagicMock

import pytest
import testing.postgresql
from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import Session

from core.config import Config, URLTypes, UserTypes
from core.db import ConfigDB
from core.models import Base, PackageManager, Source, URLType


@pytest.fixture(scope="session")
def mock_db():
    """
    Create a mock DB with necessary methods for transformer tests.
    This fixture provides consistent mock objects for URL types and sources.
    """
    db = MagicMock(spec=ConfigDB)

    # Mock URL types with consistent UUIDs
    homepage_type = MagicMock()
    homepage_type.id = uuid.UUID("00000000-0000-0000-0000-000000000001")
    repository_type = MagicMock()
    repository_type.id = uuid.UUID("00000000-0000-0000-0000-000000000002")
    documentation_type = MagicMock()
    documentation_type.id = uuid.UUID("00000000-0000-0000-0000-000000000003")
    source_type = MagicMock()
    source_type.id = uuid.UUID("00000000-0000-0000-0000-000000000004")

    db.select_url_types_by_name.side_effect = lambda name: {
        "homepage": homepage_type,
        "repository": repository_type,
        "documentation": documentation_type,
        "source": source_type,
    }[name]

    # Mock sources with consistent UUIDs
    github_source = MagicMock()
    github_source.id = uuid.UUID("00000000-0000-0000-0000-000000000005")
    crates_source = MagicMock()
    crates_source.id = uuid.UUID("00000000-0000-0000-0000-000000000006")

    db.select_source_by_name.side_effect = lambda name: {
        "github": github_source,
        "crates": crates_source,
    }[name]

    return db


@pytest.fixture(scope="session")
def url_types(mock_db):
    """Provide URL types configuration for tests."""
    return URLTypes(mock_db)


@pytest.fixture(scope="session")
def user_types(mock_db):
    """Provide user types configuration for tests."""
    return UserTypes(mock_db)


@pytest.fixture(scope="class")
def pg_db():
    """
    Create a temporary PostgreSQL database for integration tests.
    This database is recreated for each test class.
    """
    with testing.postgresql.Postgresql() as postgresql:
        yield postgresql


@pytest.fixture
def db_session(pg_db):
    """
    Create a database session using temporary PostgreSQL.
    This fixture handles database initialization and cleanup.
    """
    engine = create_engine(pg_db.url())

    # Create UUID extension for PostgreSQL
    @event.listens_for(Base.metadata, "before_create")
    def create_uuid_function(target, connection, **kw):
        connection.execute(
            text("""
            CREATE OR REPLACE FUNCTION uuid_generate_v4()
            RETURNS uuid
            AS $$
            BEGIN
                RETURN gen_random_uuid();
            END;
            $$ LANGUAGE plpgsql;
        """)
        )

    Base.metadata.create_all(engine)

    with Session(engine) as session:
        # Initialize URL types
        for url_type_name in ["homepage", "repository", "documentation", "source"]:
            existing_url_type = (
                session.query(URLType).filter_by(name=url_type_name).first()
            )
            if not existing_url_type:
                session.add(URLType(name=url_type_name))
        session.commit()

        # Initialize sources
        for source_type in ["github", "crates"]:
            existing_source = session.query(Source).filter_by(type=source_type).first()
            if not existing_source:
                session.add(Source(type=source_type))
        session.commit()

        # Initialize package manager
        crates_source = session.query(Source).filter_by(type="crates").first()
        existing_package_manager = (
            session.query(PackageManager).filter_by(source_id=crates_source.id).first()
        )
        if not existing_package_manager:
            package_manager = PackageManager(source_id=crates_source.id)
            session.add(package_manager)
            session.commit()

        yield session
        session.rollback()


@pytest.fixture
def mock_csv_reader():
    """
    Fixture to mock CSV reading functionality.
    Provides a consistent way to mock _read_csv_rows across transformer tests.
    """

    def create_mock_reader(data):
        def mock_reader(file_key):
            return [data].__iter__()

        return mock_reader

    return create_mock_reader


@pytest.fixture
def mock_config():
    """
    Fixture to mock Config object for tests.
    """
    config = MagicMock(spec=Config)

    config.exec_config = MagicMock()
    config.exec_config.test = True
    config.exec_config.no_cache = True

    return config
