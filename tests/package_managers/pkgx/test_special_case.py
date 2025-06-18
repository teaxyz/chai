"""
Test special case URL handling in PkgxTransformer.

This module tests the special_case method which handles URL transformations
for specific package sources like crates.io, x.org, and pkgx.sh.
"""

import pytest

from package_managers.pkgx.url import special_case


@pytest.mark.transformer
class TestSpecialCase:
    """Test special case URL transformations."""

    def test_special_case_crates_io(self):
        """Test that crates.io URLs are properly transformed."""
        assert special_case("crates.io/pkgx") == "https://crates.io/crates/pkgx"

    def test_special_case_x_org(self):
        """Test that x.org URLs are normalized."""
        assert special_case("x.org/ice") == "https://x.org"
        assert special_case("x.org/xxf86vm") == "https://x.org"

    def test_special_case_pkgx_sh(self):
        """Test that pkgx.sh URLs are redirected to GitHub."""
        assert special_case("pkgx.sh/pkgx") == "https://github.com/pkgxdev/pkgx"

    def test_special_case_no_slashes(self):
        """Test that URLs without slashes are returned as-is."""
        assert special_case("abseil.io") == "abseil.io"

    def test_special_case_double_slashes(self):
        """Test that URLs with double slashes are returned as-is."""
        assert special_case("github.com/awslabs/llrt") == "github.com/awslabs/llrt"
