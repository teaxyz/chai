"""
Unit tests for pkgx URL loading functionality.

Tests verify:
1. URLs already in database are not reloaded
2. New URLs are loaded correctly
3. PackageURLs not in database are loaded
4. Existing PackageURLs are updated with latest datetime
"""

from typing import Dict, List, Set, Tuple
from unittest.mock import MagicMock
from uuid import UUID

import pytest

from core.config import Config, URLTypes
from core.models import URL, Package
from package_managers.pkgx.loader import PkgxLoader
from package_managers.pkgx.transformer import Cache, Dependencies


@pytest.fixture
def test_scenarios():
    """
    Collection of test scenarios for URL loading.
    Each scenario represents a different case we want to test.
    """
    scenarios = {
        "new_urls": {
            "import_id": "github.com/certifi/python-certifi",
            "package_id": UUID("e0f18184-e743-40fb-8add-a5ccaac026a4"),
            # What transformer found
            "transformer_urls": [
                ("github.com/certifi/python-certifi", ["homepage", "source"])
            ],
            # What's in DB (nothing, completely new)
            "db_state": {"urls": {}, "package_urls": {}},
        },
        "existing_package_some_urls": {
            "import_id": "github.com/pyca/cryptography",
            "package_id": UUID("f0f18184-e743-40fb-8add-a5ccaac026a5"),
            # Transformer found homepage, source, and repository
            "transformer_urls": [
                ("github.com/pyca/cryptography", ["homepage", "source", "repository"])
            ],
            # DB only has homepage
            "db_state": {
                "urls": {
                    ("github.com/pyca/cryptography", "homepage"): {
                        "id": UUID("22222222-2222-2222-2222-222222222222"),
                        "url": "github.com/pyca/cryptography",
                    }
                },
                "package_urls": {
                    UUID("f0f18184-e743-40fb-8add-a5ccaac026a5"): [
                        {
                            "id": UUID("33333333-3333-3333-3333-333333333333"),
                            "url_id": UUID("22222222-2222-2222-2222-222222222222"),
                        }
                    ]
                },
            },
        },
        "all_urls_exist": {
            "import_id": "github.com/requests/requests",
            "package_id": UUID("a0a18184-e743-40fb-8add-a5ccaac026a6"),
            # Transformer found these
            "transformer_urls": [
                ("github.com/requests/requests", ["homepage", "source"])
            ],
            # DB has exact same ones
            "db_state": {
                "urls": {
                    ("github.com/requests/requests", "homepage"): {
                        "id": UUID("44444444-4444-4444-4444-444444444444"),
                        "url": "github.com/requests/requests",
                    },
                    ("github.com/requests/requests", "source"): {
                        "id": UUID("55555555-5555-5555-5555-555555555555"),
                        "url": "github.com/requests/requests",
                    },
                },
                "package_urls": {
                    UUID("a0a18184-e743-40fb-8add-a5ccaac026a6"): [
                        {
                            "id": UUID("66666666-6666-6666-6666-666666666666"),
                            "url_id": UUID("44444444-4444-4444-4444-444444444444"),
                        },
                        {
                            "id": UUID("77777777-7777-7777-7777-777777777777"),
                            "url_id": UUID("55555555-5555-5555-5555-555555555555"),
                        },
                    ]
                },
            },
        },
    }

    return scenarios


def test_load_urls(mock_config, mock_db, test_scenarios):
    """Test URL loading for different scenarios"""

    for scenario_name, scenario in test_scenarios.items():
        # Set up cache for this scenario
        cache = Cache(
            package=Package(id=scenario["package_id"], import_id=scenario["import_id"]),
            urls=[
                URL(url=url, url_type_id=mock_db.select_url_types_by_name(url_type).id)
                for url, url_types in scenario["transformer_urls"]
                for url_type in url_types
            ],
            dependencies=Dependencies(),
        )

        # Create cache map as expected by loader
        cache_map = {scenario["import_id"]: cache}

        # Create loader with our test data
        loader = PkgxLoader(mock_config, cache_map)

        # Mock DB state for this scenario
        current_urls_mock = MagicMock()
        current_urls_mock.url_map = scenario["db_state"]["urls"]
        current_urls_mock.package_urls = scenario["db_state"]["package_urls"]

        # define the function get_current_urls
        mock_db.get_current_urls = MagicMock()
        mock_db.get_current_urls.side_effect = lambda urls: current_urls_mock

        # also mock the session, since loader doesn't return stuff, just loads
        mock_db.session = MagicMock()
        mock_db.session.return_value.__enter__.return_value = mock_db.session

        loader.load_urls()

        # Test logic specific to each scenario
        if scenario_name == "new_package_new_urls":
            # TODO: Assert new URLs and relationships are created
            pass
        elif scenario_name == "existing_package_some_urls":
            # TODO: Assert only missing URLs and relationships are created
            pass
        elif scenario_name == "all_urls_exist":
            # TODO: Assert no new URLs created, only timestamps updated
            pass
