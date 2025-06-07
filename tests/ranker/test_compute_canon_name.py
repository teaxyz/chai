#!/usr/bin/env uv run --with pytest
import pytest

from ranker.naming import (
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
        ("@user/repo", "repo", 3),
        ("test3js", "web3.js", 8),
        ("web3", "web3.js", 11),
        ("@platonenterprise/web3", "web3.js", -3),
        ("eleventy-plugin-embed-everything", "embed-everything", 1),
        ("eleventy-plugin-embed-ted", "embed-everything", 0),
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
        # new canon, we should always have the package_name
        ("github.com/user/repo", "repo", "", "repo"),
        (
            "github.com/user/repo",
            "@scoped/random-name-123",
            "@scoped/random-name-123",
            "@scoped/random-name-123",
        ),
        (
            "gfscott.com/embed-everything",
            "eleventy-plugin-embed-everything",
            "gfscott.com/embed-everything",
            "eleventy-plugin-embed-everything",
        ),
        (
            "gfscott.com/embed-everything",
            "eleventy-plugin-embed-ted",
            "eleventy-plugin-embed-everything",
            "eleventy-plugin-embed-everything",
        ),
    ],
)
def test_compute_canon_name(url, package_name, existing_name, expected):
    assert compute_canon_name(url, package_name, existing_name) == expected
