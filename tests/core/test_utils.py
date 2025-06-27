#!/usr/bin/env pkgx uv run
"""
Comprehensive tests for core/utils.py module.

Tests cover utility functions for type conversion, query building,
environment variable handling, dictionary key conversion, and URL validation.
"""

import os
from unittest.mock import patch

import pytest

from core.utils import (
    safe_int,
    build_query_params,
    env_vars,
    convert_keys_to_snake_case,
    is_github_url,
)


class TestSafeInt:
    """Test cases for safe_int function."""

    def test_safe_int_valid_string(self):
        """Test safe_int with valid numeric string."""
        assert safe_int("123") == 123
        assert safe_int("0") == 0
        assert safe_int("-456") == -456

    def test_safe_int_empty_string(self):
        """Test safe_int with empty string returns None."""
        assert safe_int("") is None

    def test_safe_int_invalid_string(self):
        """Test safe_int with invalid string raises ValueError."""
        with pytest.raises(ValueError):
            safe_int("not_a_number")

        with pytest.raises(ValueError):
            safe_int("12.34")  # float as string

        with pytest.raises(ValueError):
            safe_int("123abc")

    def test_safe_int_whitespace_string(self):
        """Test safe_int with whitespace-only string raises ValueError."""
        with pytest.raises(ValueError):
            safe_int("   ")

    def test_safe_int_large_numbers(self):
        """Test safe_int with large numbers."""
        assert safe_int("999999999999") == 999999999999
        assert safe_int("-999999999999") == -999999999999


class TestBuildQueryParams:
    """Test cases for build_query_params function."""

    def test_build_query_params_basic(self):
        """Test basic query parameter building."""
        items = [
            {"name": "package1", "version": "1.0.0"},
            {"name": "package2", "version": "2.0.0"},
            {"name": "package3", "version": "1.0.0"},  # duplicate version
        ]
        cache = {"1.0.0": "cached_value"}
        attr = "version"

        result = build_query_params(items, cache, attr)

        # Should return unique values not in cache
        assert "2.0.0" in result
        assert "1.0.0" not in result  # Already in cache
        assert len(result) == 1

    def test_build_query_params_empty_items(self):
        """Test build_query_params with empty items list."""
        items = []
        cache = {}
        attr = "test_attr"

        result = build_query_params(items, cache, attr)

        assert result == []

    def test_build_query_params_empty_cache(self):
        """Test build_query_params with empty cache."""
        items = [
            {"id": "1", "status": "active"},
            {"id": "2", "status": "inactive"},
            {"id": "3", "status": "active"},  # duplicate status
        ]
        cache = {}
        attr = "status"

        result = build_query_params(items, cache, attr)

        # Should return unique values from items
        assert set(result) == {"active", "inactive"}
        assert len(result) == 2

    def test_build_query_params_all_cached(self):
        """Test build_query_params when all values are cached."""
        items = [
            {"name": "pkg1", "type": "lib"},
            {"name": "pkg2", "type": "app"},
        ]
        cache = {"lib": "cached1", "app": "cached2"}
        attr = "type"

        result = build_query_params(items, cache, attr)

        assert result == []

    def test_build_query_params_duplicates(self):
        """Test build_query_params removes duplicates."""
        items = [
            {"category": "web"},
            {"category": "web"},
            {"category": "desktop"},
            {"category": "web"},
            {"category": "desktop"},
        ]
        cache = {}
        attr = "category"

        result = build_query_params(items, cache, attr)

        assert set(result) == {"web", "desktop"}
        assert len(result) == 2

    def test_build_query_params_missing_attribute(self):
        """Test build_query_params when items don't have the attribute."""
        items = [
            {"name": "pkg1"},
            {"name": "pkg2"},
        ]
        cache = {}
        attr = "missing_attr"

        with pytest.raises(KeyError):
            build_query_params(items, cache, attr)


class TestEnvVars:
    """Test cases for env_vars function."""

    def test_env_vars_true_string(self):
        """Test env_vars with 'true' string."""
        with patch.dict(os.environ, {"TEST_VAR": "true"}):
            assert env_vars("TEST_VAR", "false") is True

    def test_env_vars_true_uppercase(self):
        """Test env_vars with 'TRUE' string."""
        with patch.dict(os.environ, {"TEST_VAR": "TRUE"}):
            assert env_vars("TEST_VAR", "false") is True

    def test_env_vars_one_string(self):
        """Test env_vars with '1' string."""
        with patch.dict(os.environ, {"TEST_VAR": "1"}):
            assert env_vars("TEST_VAR", "false") is True

    def test_env_vars_false_string(self):
        """Test env_vars with 'false' string."""
        with patch.dict(os.environ, {"TEST_VAR": "false"}):
            assert env_vars("TEST_VAR", "true") is False

    def test_env_vars_zero_string(self):
        """Test env_vars with '0' string."""
        with patch.dict(os.environ, {"TEST_VAR": "0"}):
            assert env_vars("TEST_VAR", "true") is False

    def test_env_vars_random_string(self):
        """Test env_vars with random string (should be False)."""
        with patch.dict(os.environ, {"TEST_VAR": "random_value"}):
            assert env_vars("TEST_VAR", "true") is False

    def test_env_vars_empty_string(self):
        """Test env_vars with empty string."""
        with patch.dict(os.environ, {"TEST_VAR": ""}):
            assert env_vars("TEST_VAR", "true") is False

    def test_env_vars_default_true(self):
        """Test env_vars with default 'true' when var not set."""
        with patch.dict(os.environ, {}, clear=True):
            assert env_vars("NONEXISTENT_VAR", "true") is True

    def test_env_vars_default_false(self):
        """Test env_vars with default 'false' when var not set."""
        with patch.dict(os.environ, {}, clear=True):
            assert env_vars("NONEXISTENT_VAR", "false") is False

    def test_env_vars_default_one(self):
        """Test env_vars with default '1' when var not set."""
        with patch.dict(os.environ, {}, clear=True):
            assert env_vars("NONEXISTENT_VAR", "1") is True

    def test_env_vars_mixed_case(self):
        """Test env_vars with mixed case values."""
        with patch.dict(os.environ, {"TEST_VAR": "True"}):
            assert env_vars("TEST_VAR", "false") is True

        with patch.dict(os.environ, {"TEST_VAR": "False"}):
            assert env_vars("TEST_VAR", "true") is False


class TestConvertKeysToSnakeCase:
    """Test cases for convert_keys_to_snake_case function."""

    def test_convert_simple_dict(self):
        """Test converting simple dictionary keys."""
        data = {
            "first-name": "John",
            "last-name": "Doe",
            "email-address": "john@example.com"
        }

        result = convert_keys_to_snake_case(data)

        expected = {
            "first_name": "John",
            "last_name": "Doe",
            "email_address": "john@example.com"
        }
        assert result == expected

    def test_convert_nested_dict(self):
        """Test converting nested dictionary keys."""
        data = {
            "user-info": {
                "first-name": "John",
                "contact-details": {
                    "phone-number": "123-456-7890",
                    "email-address": "john@example.com"
                }
            },
            "preferences": {
                "theme-color": "dark",
                "auto-save": True
            }
        }

        result = convert_keys_to_snake_case(data)

        expected = {
            "user_info": {
                "first_name": "John",
                "contact_details": {
                    "phone_number": "123-456-7890",
                    "email_address": "john@example.com"
                }
            },
            "preferences": {
                "theme_color": "dark",
                "auto_save": True
            }
        }
        assert result == expected

    def test_convert_with_list(self):
        """Test converting dictionary with list values."""
        data = {
            "user-list": [
                {"first-name": "John", "last-name": "Doe"},
                {"first-name": "Jane", "last-name": "Smith"}
            ],
            "settings": {
                "enabled-features": ["feature-one", "feature-two"]
            }
        }

        result = convert_keys_to_snake_case(data)

        expected = {
            "user_list": [
                {"first_name": "John", "last_name": "Doe"},
                {"first_name": "Jane", "last_name": "Smith"}
            ],
            "settings": {
                "enabled_features": ["feature-one", "feature-two"]
            }
        }
        assert result == expected

    def test_convert_no_hyphens(self):
        """Test converting dictionary with no hyphens."""
        data = {
            "name": "John",
            "age": 30,
            "active": True
        }

        result = convert_keys_to_snake_case(data)

        # Should return identical dict when no hyphens present
        assert result == data

    def test_convert_mixed_keys(self):
        """Test converting dictionary with mixed key formats."""
        data = {
            "first-name": "John",
            "lastName": "Doe",  # camelCase - should only replace hyphens
            "email_address": "john@example.com",  # already snake_case
            "phone-number": "123-456-7890"
        }

        result = convert_keys_to_snake_case(data)

        expected = {
            "first_name": "John",
            "lastName": "Doe",  # camelCase preserved
            "email_address": "john@example.com",
            "phone_number": "123-456-7890"
        }
        assert result == expected

    def test_convert_empty_dict(self):
        """Test converting empty dictionary."""
        data = {}
        result = convert_keys_to_snake_case(data)
        assert result == {}

    def test_convert_dict_with_non_dict_values(self):
        """Test converting dictionary containing various value types."""
        data = {
            "string-key": "string value",
            "number-key": 123,
            "boolean-key": True,
            "none-key": None,
            "list-key": ["item1", "item2"]
        }
        
        result = convert_keys_to_snake_case(data)
        
        expected = {
            "string_key": "string value",
            "number_key": 123,
            "boolean_key": True,
            "none_key": None,
            "list_key": ["item1", "item2"]
        }
        assert result == expected

    def test_convert_multiple_hyphens(self):
        """Test converting keys with multiple hyphens."""
        data = {
            "very-long-key-name": "value1",
            "another--double-hyphen": "value2",
            "single-hyphen": "value3"
        }

        result = convert_keys_to_snake_case(data)

        expected = {
            "very_long_key_name": "value1",
            "another__double_hyphen": "value2",
            "single_hyphen": "value3"
        }
        assert result == expected


class TestIsGithubUrl:
    """Test cases for is_github_url function."""

    def test_is_github_url_valid(self):
        """Test is_github_url with valid GitHub URLs."""
        assert is_github_url("github.com/user/repo") is True
        assert is_github_url("github.com/organization/project") is True
        assert is_github_url("github.com/user/repo.git") is True

    def test_is_github_url_with_path(self):
        """Test is_github_url with GitHub URLs containing paths."""
        assert is_github_url("github.com/user/repo/issues") is True
        assert is_github_url("github.com/user/repo/tree/main") is True
        assert is_github_url("github.com/user/repo/blob/main/README.md") is True

    def test_is_github_url_invalid(self):
        """Test is_github_url with non-GitHub URLs."""
        assert is_github_url("gitlab.com/user/repo") is False
        assert is_github_url("bitbucket.org/user/repo") is False
        assert is_github_url("example.com/path") is False
        assert is_github_url("https://github.com/user/repo") is False  # Has protocol

    def test_is_github_url_empty(self):
        """Test is_github_url with empty string."""
        assert is_github_url("") is False

    def test_is_github_url_partial_match(self):
        """Test is_github_url with partial matches."""
        assert is_github_url("mygithub.com/user/repo") is False
        assert is_github_url("github.com.evil.com/user/repo") is False
        assert is_github_url("subdomain.github.com/user/repo") is False

    def test_is_github_url_case_sensitive(self):
        """Test is_github_url is case sensitive."""
        assert is_github_url("GitHub.com/user/repo") is False
        assert is_github_url("GITHUB.COM/user/repo") is False

    def test_is_github_url_whitespace(self):
        """Test is_github_url with whitespace."""
        assert is_github_url(" github.com/user/repo") is False
        assert is_github_url("github.com/user/repo ") is False
        assert is_github_url(" github.com/user/repo ") is False


# Integration tests
class TestUtilsIntegration:
    """Integration tests for utility functions."""

    def test_env_vars_with_convert_keys(self):
        """Test combining env_vars with convert_keys_to_snake_case."""
        # Simulate reading config that might have hyphenated keys
        with patch.dict(os.environ, {"ENABLE_FEATURE": "true"}):
            config_data = {
                "feature-enabled": env_vars("ENABLE_FEATURE", "false"),
                "debug-mode": False
            }
            
            result = convert_keys_to_snake_case(config_data)
            
            expected = {
                "feature_enabled": True,
                "debug_mode": False
            }
            assert result == expected

    def test_build_query_params_with_safe_int(self):
        """Test combining build_query_params with safe_int."""
        items = [
            {"id": "1", "port": "8080"},
            {"id": "2", "port": "9000"},
            {"id": "3", "port": ""},  # Empty port
        ]
        
        # Extract ports and convert safely
        ports = []
        for item in items:
            port = safe_int(item["port"])
            if port is not None:
                ports.append(port)
        
        assert ports == [8080, 9000]
        
        # Use with query building
        cache = {8080: "cached"}
        port_items = [{"port": str(p)} for p in ports]
        
        result = build_query_params(port_items, cache, "port")
        assert result == ["9000"]