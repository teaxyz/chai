import unittest
from unittest.mock import MagicMock
from uuid import uuid4

from core.config import Config, DependencyTypes, PackageManagers, PMConf, URLTypes
from package_managers.pkgx.transformer import PkgxTransformer


class TestSpecialCase(unittest.TestCase):
    def setUp(self):
        """Set up common test data and mocks"""

        self.crates_package_manager_id = uuid4()

        # mock config values
        self.mock_dep_types = MagicMock(spec=DependencyTypes)
        self.mock_url_types = MagicMock(spec=URLTypes)
        self.mock_pm_config = MagicMock(spec=PMConf)
        self.mock_pm_config.pm_id = self.crates_package_manager_id
        self.mock_package_managers = MagicMock(spec=PackageManagers)

        # mock the config
        self.mock_config = MagicMock(spec=Config)
        self.mock_config.pm_config = self.mock_pm_config
        self.mock_config.package_managers = self.mock_package_managers
        self.mock_config.dependency_types = self.mock_dep_types
        self.mock_config.url_types = self.mock_url_types

        self.transformer = PkgxTransformer(self.mock_config, None)

    def test_special_case_crates_io(self):
        self.assertEqual(
            self.transformer.special_case("crates.io/pkgx"),
            "https://crates.io/crates/pkgx",
        )

    def test_special_case_x_org(self):
        self.assertEqual(self.transformer.special_case("x.org/ice"), "https://x.org")
        self.assertEqual(
            self.transformer.special_case("x.org/xxf86vm"), "https://x.org"
        )

    def test_special_case_pkgx_sh(self):
        self.assertEqual(
            self.transformer.special_case("pkgx.sh/pkgx"),
            "https://github.com/pkgxdev/pkgx",
        )

    def test_special_case_no_slashes(self):
        self.assertEqual(self.transformer.special_case("abseil.io"), "abseil.io")

    def test_special_case_double_slashes(self):
        self.assertEqual(
            self.transformer.special_case("github.com/awslabs/llrt"),
            "github.com/awslabs/llrt",
        )
