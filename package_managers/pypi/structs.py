from enum import Enum, auto


class DependencyType(Enum):
    """
    Enum representing different types of PyPI dependencies.
    Based on https://packaging.python.org/en/latest/specifications/dependency-specifiers/
    """
    REQUIRES = auto()  # Regular dependencies
    REQUIRES_PYTHON = auto()  # Python version requirement
    EXTRA_REQUIRES = auto()  # Optional dependencies
    TEST_REQUIRES = auto()  # Test dependencies
    DEV_REQUIRES = auto()  # Development dependencies
