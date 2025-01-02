"""
System tests for the complete data pipeline.

These tests verify the entire system working together with:
1. Real PostgreSQL database
2. Actual data transformations
3. End-to-end data flow

These tests require the full Docker Compose setup and are skipped
if the required environment is not available.
"""

import os

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from core.models import Base


@pytest.mark.system
class TestSystemIntegration:
    """
    System tests that require the full Docker Compose setup.
    These tests verify the entire system working together.
    """

    def is_postgres_ready(self):
        """
        Check if PostgreSQL is available.

        Returns:
            bool: True if PostgreSQL is accessible, False otherwise
        """
        try:
            engine = create_engine(
                os.environ.get(
                    "CHAI_DATABASE_URL",
                    "postgresql://postgres:s3cr3t@localhost:5435/chai",
                )
            )
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except Exception as e:
            print(f"PostgreSQL not ready: {e}")
            return False

    @pytest.fixture
    def db_session(self):
        """
        Create a PostgreSQL database session.

        This fixture:
        1. Checks if PostgreSQL is available
        2. Creates all tables if they don't exist
        3. Provides a session for the test
        4. Rolls back changes after the test
        """
        if not self.is_postgres_ready():
            pytest.skip("PostgreSQL is not available")

        engine = create_engine(os.environ.get("CHAI_DATABASE_URL"))
        Base.metadata.create_all(engine)

        with Session(engine) as session:
            yield session
            session.rollback()

    @pytest.mark.skipif(
        not os.environ.get("RUN_SYSTEM_TESTS"), reason="System tests not enabled"
    )
    def test_full_pipeline(self, db_session):
        """
        Test the entire pipeline with actual database.

        This test verifies:
        1. Data loading from CSV files
        2. Transformation of raw data
        3. Database schema compatibility
        4. Data integrity across models
        """
        # TODO: Implement full pipeline test
        pass
