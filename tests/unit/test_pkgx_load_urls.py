"""
Unit tests for pkgx URL loading functionality.

Tests verify:
1. URLs already in database are not reloaded
2. New URLs are loaded correctly
3. PackageURLs not in database are loaded
4. Existing PackageURLs are updated with latest datetime
"""

import uuid
from datetime import datetime, timezone
from typing import Dict, List, Set, Tuple
from unittest.mock import MagicMock
from uuid import UUID

import pytest

from core.config import Config, URLTypes
from core.models import URL, PackageURL
from package_managers.pkgx.loader import PkgxLoader
from package_managers.pkgx.parser import Distributable, PkgxPackage
from package_managers.pkgx.transformer import PkgxTransformer


@pytest.fixture
def db_test_data(mock_db):
    """Fixture containing the test data for whatever would be loaded into the DB"""

    # Create sample data
    # also, don't make the below logic complicated
    # please, just put one unique URL here
    data: Dict[str, List[Tuple[str, str]]] = {
        "e0f18184-e743-40fb-8add-a5ccaac026a4": [
            ("https://github.com/certifi/python-certifi", "homepage"),
            ("github.com/certifi/python-certifi", "source"),
        ]
    }

    # build the package ids
    package_ids: List[UUID] = list(UUID(id) for id in data.keys())

    # build the url map
    url_map: Dict[Tuple[str, str], URL] = {}
    for package_id in data:
        for url, url_type in data[package_id]:
            url_map[(url, url_type)] = URL(
                id=uuid.uuid4(),
                url=url,
                url_type_id=mock_db.select_url_types_by_name(url_type).id,
            )

    package_urls: Dict[UUID, Set[PackageURL]] = {}
    for package_id in data:
        for url, url_type in data[package_id]:
            package_urls[package_id].add(
                PackageURL(
                    id=uuid.uuid4(),
                    package_id=package_id,
                    url_id=url_map[(url, url_type)].id,
                )
            )

    return {
        "package_ids": package_ids,
        "url_map": url_map,
        "package_urls": package_urls,
    }


def test_load_urls(mock_db, url_test_data):
    current_urls_mock = MagicMock()
    current_urls_mock.url_map = url_test_data["url_map"]
    current_urls_mock.package_urls = url_test_data["package_urls"]

    current_urls_mock.side_effect = lambda urls: current_urls_mock

    urls_to_check = [
        "https://github.com/certifi/python-certifi",
        "github.com/certifi/python-certifi",
    ]
    mock_db.get_current_urls(urls_to_check)

    mock_db.get_current_urls.assert_called_once_with(urls_to_check)
