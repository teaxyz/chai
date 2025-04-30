from unittest.mock import Mock

import pytest

from scripts.package_to_package.package_dependencies import get_latest_version_info


def create_mock_version(version_str: str) -> Mock:
    mock = Mock()
    mock.version = version_str
    return mock


@pytest.mark.parametrize(
    "version_list_strs, expected_latest_str",
    [
        # === Basic Cases ===
        ([], None),
        (["gibberish"], "gibberish"),  # because only one entry
        (["1.0.0", "1.1.0", "1.0.1"], "1.1.0"),
        (["1.1.0", "1.0.0", "0.9.0"], "1.1.0"),
        (["1.0", "1.0.1"], "1.0.1"),
        (["1.2", "1.1.0"], "1.2"),
        (["2", "1.9.9"], "2"),
        (["1.4rc5", "1.3"], "1.4rc5"),
        (["004", "3"], "004"),
        (["11-062", "10.0"], "11-062"),
        (["1.0b3", "1.0b2"], "1.0b3"),
        (["1.0.0", "1.4rc5", "1.1.0"], "1.4rc5"),
        (["1.9.0", "2.6.4.3", "1.10.0", "5.2.10.0"], "5.2.10.0"),
        (["0.10.0", "0.11.0.0", "0.9.0"], "0.11.0.0"),
        (["3.0.0", "3.01", "2.9.9"], "3.01"),
        (["1.0.8", "1.09", "1.0.0"], "1.09"),
        (["1.3.0", "1.3.0rc1", "1.2.9"], "1.3.0"),
        (["0.1.0", "019", "0.0.1"], "019"),
        (["1.0.0", "1.07.1", "0.9.9"], "1.07.1"),
        (["3.0.0", "3.07", "2.9.9"], "3.07"),
        (["4.6a", "4.5b"], "4.6a"),
        (["25.0.0", "25.03", "24.9.9"], "25.03"),
        (["0.9.0", "0.H", "0.8.0"], "0.9.0"),
        (["4.0.0", "4.07", "3.9.9"], "4.07"),
        (["5.9.0", "5.09.03", "5.8.9"], "5.09.03"),
        (["0.58", "0.58b"], "0.58b"),
        (["1.4rc5", "1.0.0", "2025.01.13", "1.1.0", "2.4h"], "2025.01.13"),
        (["2.6.4.3", "5.2.10.0", "0.11.0.0", "1.0b3", "11-062"], "11-062"),
        # YYYY-MM-DD
        (["2025.01.13", "2024.12.31"], "2025.01.13"),
        (["2024-11-26", "2024-12-31", "2025-02-25"], "2025-02-25"),
        (["2024.11.25", "2024-11-26"], "2024-11-26"),  # Mix with standard
        (["2024-11-26"], "2024-11-26"),  # Single version bypass
        # YYYYMMDDTHHMMSS
        (
            ["20241108T174929", "20241108.174928"],
            "20241108T174929",
        ),  # Preprocessed to YYYYMMDD.HHMMSS
        (["20241109T000000", "20241108.235959"], "20241109T000000"),
        (["20241108T174929"], "20241108T174929"),  # Single version bypass
        # YYYY.MM.DD-HH.MM.SS
        (
            ["2025.03.27-20.21.36", "2025.03.27+202135"],
            "2025.03.27-20.21.36",
        ),  # Preprocessed to YYYY.MM.DD+HHMMSS
        # Base version > with local identifier
        (["2025.03.27-20.21.36"], "2025.03.27-20.21.36"),  # Single version bypass
        # === New Preprocessing Test Cases ===
        (["4.7w", "4.7x"], "4.7x"),
        (["4.7w", "4.7"], "4.7w"),
        (
            ["0.16.2-gitlab.30", "0.16.2-gitlab.31", "0.16.2-gitlab.35"],
            "0.16.2-gitlab.35",
        ),
        (["3.1.3-p1", "3.2.2"], "3.2.2"),
        (["9.4.56.v20240826", "9.4.57.v20241219"], "9.4.57.v20241219"),
        (["9.4.56", "9.4.56.v20240826"], "9.4.56.v20240826"),  # +local > base
        (["20240829-update1", "20240829-update2"], "20240829-update2"),
        (["20240829", "20240829-update1"], "20240829-update1"),
        (["20240808-3.1", "20250104-3.1"], "20250104-3.1"),
        (["20240808", "20240808-3.1"], "20240808-3.1"),
        (["2.8.9rel.1", "2.8.9"], "2.8.9rel.1"),
        (["2025-04-08T15-41-24Z", "2025-04-08T15-39-49Z"], "2025-04-08T15-41-24Z"),
        (["16-747c6", "17-b804f"], "17-b804f"),
        (["16", "16-747c6"], "16-747c6"),
        (["9.9p1", "9.9p2"], "9.9p2"),
        (["9.9", "9.9p1"], "9.9p1"),
        (["2024_12_11.09478d5", "2025_03_20.32f6212"], "2025_03_20.32f6212"),
        (["4.4.0p8", "4.4.0"], "4.4.0p8"),
        (["p6.1.20241222.0", "p6.1.20250112.0"], "p6.1.20250112.0"),
        (
            ["6.1.20241222.0", "p6.1.20241222.0"],
            "6.1.20241222.0",
        ),  # Base > preprocessed p-stripped
        (["r1951", "r1980"], "r1980"),
        (["0.1", "r1951"], "0.1"),  # Version 0.1 > 0+r1951
        (["9.8z", "9.8za"], "9.8za"),
        (["9.8", "9.8z"], "9.8z"),
        (["0.5.3-git20230121", "0.5.3"], "0.5.3"),
        (["4.3ga10", "4.4ga5"], "4.4ga5"),
        (["4.3", "4.3ga10"], "4.3ga10"),
        (
            [
                "0.1.13",
                "0.3.0",
                "0.3.0-nightly.3",
                "0.3.0-nightly.2",
                "0.3.1-nightly",
                "0.3.0-nightly",
                "0.3.0-nightly.4",
            ],
            "0.3.0-nightly.4",
        ),
        (["1.1.1s", "1.1.1t", "1.1.1u"], "1.1.1u"),
    ],
)
def test_get_latest_version_info(version_list_strs, expected_latest_str):
    """
    Tests the get_latest_version_info function with various version string formats.
    """
    # Create mock Version objects from the strings
    mock_versions = [create_mock_version(v_str) for v_str in version_list_strs]

    # Call the function under test
    latest_version_obj = get_latest_version_info(mock_versions)

    # Assert the result
    if expected_latest_str is None:
        assert latest_version_obj is None
    else:
        assert (
            latest_version_obj is not None
        ), f"No latest version found for {version_list_strs}"  # noqa
        assert latest_version_obj.version == expected_latest_str
