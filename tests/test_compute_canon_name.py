#!/usr/bin/env uv run --with pytest
from core.logger import Logger
from ranker.canon_names import (
    compute_canon_name,
    extract_repo_name_from_url,
    find_best_package_name,
)


class TestExtractRepoNameFromUrl:
    """Test URL parsing to extract repository names."""

    def test_github_url(self):
        assert extract_repo_name_from_url("github.com/chjj/marked") == "marked"
        assert extract_repo_name_from_url("github.com/user/repo-name") == "repo-name"

    def test_github_url_with_trailing_slash(self):
        assert extract_repo_name_from_url("github.com/chjj/marked/") == "marked"

    def test_non_github_url(self):
        assert (
            extract_repo_name_from_url("gitlab.com/user/project")
            == "gitlab.com/user/project"
        )
        assert (
            extract_repo_name_from_url("bitbucket.org/user/repo")
            == "bitbucket.org/user/repo"
        )

    def test_malformed_url(self):
        # Should not crash and return the original URL
        result = extract_repo_name_from_url("not-a-valid-url")
        assert result == "not-a-valid-url"


class TestFindBestPackageName:
    """Test package name selection heuristics."""

    def test_exact_match_with_repo_name(self):
        # When one package name exactly matches the repo name
        packages = ["marked-prettyprint", "marked", "marked-edp"]
        url = "https://github.com/chjj/marked"
        assert find_best_package_name(packages, url) == "marked"

    def test_no_exact_match_prefer_similar(self):
        # When no exact match, prefer names containing the repo name
        packages = ["marked-prettyprint", "marked-edp", "other-package"]
        url = "https://github.com/chjj/marked"
        # Should prefer one of the marked-* packages
        result = find_best_package_name(packages, url)
        assert "marked" in result.lower()

    def test_prefer_shorter_names(self):
        # When multiple names contain the repo name, prefer shorter ones
        packages = ["marked-with-custom-heading-ids", "marked-edp", "marked"]
        url = "https://github.com/chjj/marked"
        assert find_best_package_name(packages, url) == "marked"

    def test_penalize_scoped_packages(self):
        # Scoped packages should be penalized
        packages = ["@hellstad/marked", "marked-edp", "marked-prettyprint"]
        url = "https://github.com/chjj/marked"
        result = find_best_package_name(packages, url)
        # Should not pick the scoped package if alternatives exist
        assert not result.startswith("@")

    def test_empty_list(self):
        assert find_best_package_name([], "https://github.com/user/repo") is None

    def test_single_package(self):
        packages = ["some-package"]
        url = "https://github.com/user/repo"
        assert find_best_package_name(packages, url) == "some-package"


class TestComputeCanonName:
    """Test the main compute_canon_name function."""

    def test_monorepo_detection(self):
        # When >20 packages, should return URL
        packages = [f"package-{i}" for i in range(25)]  # 25 packages
        result = compute_canon_name(
            current_package_name="package-0",
            canon_url="https://github.com/microsoft/monorepo",
            existing_canon_name=None,
            packages_for_canon=packages[1:],  # Don't include current package in list
        )
        assert result == "https://github.com/microsoft/monorepo"

    def test_marked_example_scenario(self):
        # The real-world "marked" example from the user's description
        marked_packages = [
            "marked-prettyprint",
            "marked2",
            "marked-edp",
            "@hellstad/marked",
            "marked-papandreou",
            "marked-component",
            "markdn",
            "@npm-polymer/marked",
            "georgerogers42-marked",
            "marked-with-custom-heading-ids",
            "mdto",
            "mamarked",
            "@weo-edu/marked",
            "tb.marked",
            "marked-lianyue",
            "appc-marked",
        ]

        # Test when "marked" is NOT in the package list (realistic scenario)
        result = compute_canon_name(
            current_package_name="marked-prettyprint",
            canon_url="https://github.com/chjj/marked",
            existing_canon_name=None,
            packages_for_canon=marked_packages[1:],  # Don't include current in list
        )

        # Should pick one of the better marked variants, not a scoped one
        assert "marked" in result.lower()
        assert not result.startswith("@")  # Should avoid scoped packages

    def test_marked_example_with_canonical_name(self):
        # If "marked" was actually in the package list, it should be chosen
        marked_packages = [
            "marked-prettyprint",
            "marked",
            "marked-edp",
            "@hellstad/marked",
            "marked-papandreou",
        ]

        result = compute_canon_name(
            current_package_name="marked-prettyprint",
            canon_url="https://github.com/chjj/marked",
            existing_canon_name=None,
            packages_for_canon=marked_packages[1:],
        )

        assert result == "marked"

    def test_new_canon_single_package(self):
        # When creating a new canon with just one package
        result = compute_canon_name(
            current_package_name="lodash",
            canon_url="https://github.com/lodash/lodash",
            existing_canon_name=None,
            packages_for_canon=[],
        )

        assert result == "lodash"

    def test_existing_canon_better_name_available(self):
        # When an existing canon has a poor name but we can pick a better one
        result = compute_canon_name(
            current_package_name="react",
            canon_url="https://github.com/facebook/react",
            existing_canon_name="@scoped/react-fork",
            packages_for_canon=["@scoped/react-fork", "react-dom"],
        )

        # Should pick "react" as it's the best name
        assert result == "react"

    def test_fallback_to_url(self):
        # When no good package name can be determined
        result = compute_canon_name(
            current_package_name="xyz123abc",
            canon_url="https://example.com/some/weird/url",
            existing_canon_name=None,
            packages_for_canon=["abc123xyz", "random-name-456"],
        )

        # Should fall back to URL when no good name exists
        assert result == "https://example.com/some/weird/url"

    def test_with_logger(self):
        # Test that logging works without errors
        logger = Logger("test")
        result = compute_canon_name(
            current_package_name="test-package",
            canon_url="https://github.com/user/test",
            existing_canon_name=None,
            packages_for_canon=["other-package"],
            logger=logger,
        )

        # Should not crash and return a reasonable result
        assert result in [
            "test-package",
            "other-package",
            "https://github.com/user/test",
        ]


# Example usage and manual testing
if __name__ == "__main__":
    """
    Manual test scenarios to understand the function behavior.
    Run with: python -m pytest tests/test_compute_canon_name.py -v
    Or run this file directly to see example outputs.
    """

    logger = Logger("test")

    print("=== MARKED EXAMPLE ===")
    marked_packages = [
        "marked-prettyprint",
        "marked2",
        "marked-edp",
        "@hellstad/marked",
        "marked-papandreou",
        "marked-component",
        "markdn",
        "@npm-polymer/marked",
        "georgerogers42-marked",
        "marked-with-custom-heading-ids",
        "mdto",
        "mamarked",
        "@weo-edu/marked",
        "tb.marked",
        "marked-lianyue",
        "appc-marked",
    ]

    result1 = compute_canon_name(
        "marked-prettyprint",
        "https://github.com/chjj/marked",
        None,
        marked_packages[1:],
        logger,
    )
    print(f"Result for marked scenario: {result1}")

    print("\n=== MONOREPO EXAMPLE ===")
    big_packages = [f"package-{i}" for i in range(25)]
    result2 = compute_canon_name(
        "package-0",
        "https://github.com/microsoft/typescript",
        None,
        big_packages[1:],
        logger,
    )
    print(f"Result for monorepo scenario: {result2}")

    print("\n=== SIMPLE CASE ===")
    result3 = compute_canon_name(
        "lodash", "https://github.com/lodash/lodash", None, [], logger
    )
    print(f"Result for simple case: {result3}")
