import unittest
from unittest.mock import MagicMock
from uuid import uuid4

from core.config import Config, DependencyTypes, PackageManagers, PMConf, URLTypes
from package_managers.pkgx.transformer import PkgxTransformer


class TestSpecialCase(unittest.TestCase):
    def setUp(self):
        """Set up common test data and mocks"""

        # Create predefined UUIDs for dependency types
        self.runtime_type_id = uuid4()
        self.build_type_id = uuid4()
        self.test_type_id = uuid4()
        self.recommended_type_id = uuid4()
        self.optional_type_id = uuid4()

        # Create mock dependency types
        self.mock_dep_types = MagicMock(spec=DependencyTypes)
        self.mock_dep_types.runtime = self.runtime_type_id
        self.mock_dep_types.build = self.build_type_id
        self.mock_dep_types.test = self.test_type_id
        self.mock_dep_types.recommended = self.recommended_type_id
        self.mock_dep_types.optional = self.optional_type_id

        # Create predefined UUIDs for URL types
        self.homepage_type_id = uuid4()
        self.repository_type_id = uuid4()
        self.documentation_type_id = uuid4()
        self.source_type_id = uuid4()

        # Create mock URL types
        self.mock_url_types = MagicMock(spec=URLTypes)
        self.mock_url_types.homepage = self.homepage_type_id
        self.mock_url_types.repository = self.repository_type_id
        self.mock_url_types.documentation = self.documentation_type_id
        self.mock_url_types.source = self.source_type_id

        # Create predefined UUIDs for package managers
        self.crates_package_manager_id = uuid4()

        # Create mock package manager configuration
        self.mock_pm_config = MagicMock(spec=PMConf)
        self.mock_pm_config.pm_id = self.crates_package_manager_id

        # Create the package managers
        self.mock_package_managers = MagicMock(spec=PackageManagers)
        self.mock_package_managers.crates = self.crates_package_manager_id

        # Create a mock Config that returns our mock dependency types and URL types
        self.mock_config = MagicMock(spec=Config)
        self.mock_config.dependency_types = self.mock_dep_types
        self.mock_config.url_types = self.mock_url_types
        self.mock_config.pm_config = self.mock_pm_config
        self.mock_config.package_managers = self.mock_package_managers

    def test_special_case_crates_io(self):
        transformer = PkgxTransformer(self.mock_config, None)
        self.assertEqual(
            transformer.special_case("crates.io/pkgx"),
            "https://crates.io/crates/pkgx"
        )

    def test_special_case_x_org(self):
        transformer = PkgxTransformer(self.mock_config, None)
        self.assertEqual(transformer.special_case("x.org/ice"), "https://x.org")
        self.assertEqual(transformer.special_case("x.org/xxf86vm"), "https://x.org")

    def test_special_case_pkgx_sh(self):
        transformer = PkgxTransformer(self.mock_config, None)
        self.assertEqual(
            transformer.special_case("pkgx.sh/pkgx"),
            "https://github.com/pkgxdev/pkgx"
        )

    def test_special_case_no_slashes(self):
        transformer = PkgxTransformer(self.mock_config, None)
        self.assertEqual(transformer.special_case("abseil.io"), "abseil.io")

    def test_special_case_double_slashes(self):
        transformer = PkgxTransformer(self.mock_config, None)
        self.assertEqual(
            transformer.special_case("github.com/awslabs/llrt"),
            "github.com/awslabs/llrt"
        )
