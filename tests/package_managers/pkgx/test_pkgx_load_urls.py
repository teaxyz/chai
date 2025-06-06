# TODO: need to fix this test, since it depends on the DB URL being available

"""
Unit tests for pkgx URL loading functionality.

Tests verify:
1. URLs already in database are not reloaded
2. New URLs are loaded correctly
3. PackageURLs not in database are loaded
4. Existing PackageURLs are updated with latest datetime
"""

from unittest.mock import MagicMock
from uuid import UUID

import pytest

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
            "expected_behavior": {
                "new_urls_created": 2,  # homepage and source
                "new_package_urls_created": 2,
                "urls_updated": 0,
            },
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
            "expected_behavior": {
                "new_urls_created": 2,  # source and repository
                "new_package_urls_created": 2,
                "urls_updated": 1,  # homepage timestamp updated
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
            "expected_behavior": {
                "new_urls_created": 0,
                "new_package_urls_created": 0,
                "urls_updated": 2,  # Both timestamps updated
            },
        },
        "no_urls_in_db": {
            "import_id": "github.com/numpy/numpy",
            "package_id": UUID("b0b18184-e743-40fb-8add-a5ccaac026a7"),
            # Transformer found URLs but DB has no record of this package
            "transformer_urls": [
                ("github.com/numpy/numpy", ["homepage", "repository", "documentation"])
            ],
            # DB is empty for this package
            "db_state": {"urls": {}, "package_urls": {}},
            "expected_behavior": {
                "new_urls_created": 3,
                "new_package_urls_created": 3,
                "urls_updated": 0,
            },
        },
    }

    return scenarios


@pytest.mark.loader
class TestPkgxLoader:
    """Test the PkgxLoader URL loading functionality."""

    def test_load_urls(self, mock_config, test_scenarios):
        """Test URL loading for different scenarios."""
        for scenario_name, scenario in test_scenarios.items():
            # Set up cache for this scenario
            cache = Cache(
                package=Package(
                    id=scenario["package_id"], import_id=scenario["import_id"]
                ),
                urls=[
                    URL(
                        url=url,
                        url_type_id=mock_config.db.select_url_types_by_name(
                            url_type
                        ).id,
                    )
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

            # Mock the get_current_urls method
            mock_config.db.get_current_urls = MagicMock()
            mock_config.db.get_current_urls.return_value = current_urls_mock

            # Mock the session for loader operations
            mock_config.db.session = MagicMock()
            mock_session = MagicMock()
            mock_config.db.session.return_value.__enter__.return_value = mock_session

            # Track calls to verify behavior
            urls_added = []
            package_urls_added = []
            urls_updated = []

            def track_add(obj):
                if hasattr(obj, "url"):  # It's a URL object
                    urls_added.append(obj)
                else:  # It's a PackageURL object
                    package_urls_added.append(obj)

            def track_bulk_update(mapper, mappings):
                urls_updated.extend(mappings)

            mock_session.add.side_effect = track_add
            mock_session.bulk_update_mappings.side_effect = track_bulk_update

            # Run the loader
            loader.load_urls()

            # Verify expected behavior
            expected = scenario["expected_behavior"]
            assert (
                len(urls_added) == expected["new_urls_created"]
            ), f"Scenario {scenario_name}: Expected {expected['new_urls_created']} new URLs, got {len(urls_added)}"  # noqa: E501
            assert (
                len(package_urls_added) == expected["new_package_urls_created"]
            ), f"Scenario {scenario_name}: Expected {expected['new_package_urls_created']} new PackageURLs, got {len(package_urls_added)}"  # noqa: E501

            # URLs updated is tracked through bulk_update_mappings calls
            if expected["urls_updated"] > 0:
                assert mock_session.bulk_update_mappings.called, f"Scenario {scenario_name}: Expected bulk_update_mappings to be called"  # noqa: E501
                # Check that the right number of URLs were updated
                total_updated = sum(
                    len(call[0][1])
                    for call in mock_session.bulk_update_mappings.call_args_list
                )
                assert (
                    total_updated == expected["urls_updated"]
                ), f"Scenario {scenario_name}: Expected {expected['urls_updated']} URLs updated, got {total_updated}"  # noqa: E501
