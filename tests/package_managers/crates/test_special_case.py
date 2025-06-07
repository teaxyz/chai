"""
Test special case URL handling in PkgxTransformer.

This module tests the special_case method which handles URL transformations
for specific package sources like crates.io, x.org, and pkgx.sh.
"""

import pytest

from package_managers.pkgx.transformer import PkgxTransformer


@pytest.mark.transformer
class TestSpecialCase:
    """Test special case URL transformations."""

    @pytest.fixture(autouse=True)
    def setup(self, mock_config):
        """Set up transformer for each test."""
        self.transformer = PkgxTransformer(mock_config, None)

    def test_special_case_crates_io(self):
        """Test that crates.io URLs are properly transformed."""
        assert (
            self.transformer.special_case("crates.io/pkgx")
            == "https://crates.io/crates/pkgx"
        )

    def test_special_case_x_org(self):
        """Test that x.org URLs are normalized."""
        assert self.transformer.special_case("x.org/ice") == "https://x.org"
        assert self.transformer.special_case("x.org/xxf86vm") == "https://x.org"

    def test_special_case_pkgx_sh(self):
        """Test that pkgx.sh URLs are redirected to GitHub."""
        assert (
            self.transformer.special_case("pkgx.sh/pkgx")
            == "https://github.com/pkgxdev/pkgx"
        )

    def test_special_case_no_slashes(self):
        """Test that URLs without slashes are returned as-is."""
        assert self.transformer.special_case("abseil.io") == "abseil.io"

    def test_special_case_double_slashes(self):
        """Test that URLs with double slashes are returned as-is."""
        assert (
            self.transformer.special_case("github.com/awslabs/llrt")
            == "github.com/awslabs/llrt"
        )
