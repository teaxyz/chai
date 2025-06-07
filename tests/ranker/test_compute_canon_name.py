#!/usr/bin/env uv run --with pytest
import pytest

from ranker.canon_names import (
    check_if_better,
    compute_canon_name,
    extract_repo_name_from_url,
    score_name,
)


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
    "name, best_guess, expected_score",
    [
        ("@user/repo", "repo", 8),
        ("test3js", "web3.js", 13),
        ("web3", "web3.js", 16),
        ("@platonenterprise/web3", "web3.js", -3),
    ],
)
def test_score_name(name, best_guess, expected_score):
    assert score_name(name, best_guess) == expected_score


@pytest.mark.parametrize(
    "name, best_guess, package_name, expected",
    [
        (
            "web3.js",
            "test3js",
            "https://github.com/ethereum/web3.js#readmeweb3.js",
            "test3js",
        ),
        ("web3.js", "web3", "test3js", "web3"),
        ("web3.js", "@platonenterprise/web3", "web3", "web3"),
    ],
)
def test_check_if_better(name, best_guess, package_name, expected):
    assert check_if_better(name, best_guess, package_name) == expected


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
    assert compute_canon_name(url, package_name, existing_name) == expected
