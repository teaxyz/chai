#!/usr/bin/env uv run --with pytest
import pytest

from ranker.canon_names import compute_canon_name_v2, extract_repo_name_from_url


@pytest.mark.parametrize(
    "url, best_guess",
    [
        ("github.com/user/repo", "repo"),
        ("gitlab.com/user/repo", "repo"),
        ("bitbucket.org/user/repo", "repo"),
        ("not-a-valid-url", "not-a-valid-url"),
        ("", ""),
    ],
)
def test_extract_repo_name_from_url(url, best_guess):
    assert extract_repo_name_from_url(url) == best_guess


@pytest.mark.parametrize(
    "url, package_name, existing_name, expected",
    [
        # all the new canons, with no existing name cases
        ("github.com/user/repo", "repo", "", "repo"),
        ("github.com/user/repo", "@user/repo", "", "repo"),
        (
            "gfscott.com/embed-everything",
            "eleventy-plugin-embed-everything",
            "",
            "eleventy-plugin-embed-everything",
        ),
        (
            "github.com/bywhitebird/whitebird",
            "eslint-plugin-whitebird",
            "",
            "eslint-plugin-whitebird",
        ),
        (
            "github.com/bywhitebird/whitebird",
            "@whitebird/eslint-config",
            "",
            "github.com/bywhitebird/whitebird",
        ),
    ],
)
def test_compute_canon_name_v2(url, package_name, existing_name, expected):
    assert compute_canon_name_v2(url, package_name, existing_name) == expected
