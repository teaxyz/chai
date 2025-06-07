# CHAI Test Suite

This directory contains the test suite for the CHAI package indexer. All tests are written using [pytest](https://docs.pytest.org/) and follow a consistent structure to ensure maintainability and ease of use.

## Table of Contents

- [Directory Structure](#directory-structure)
- [Running Tests](#running-tests)
- [Adding New Tests](#adding-new-tests)
- [Test Coverage](#test-coverage)
- [Fixtures and Mocking](#fixtures-and-mocking)
- [Test Markers](#test-markers)

## Directory Structure

The test suite is organized to mirror the main project structure:

```
tests/
├── conftest.py              # Common fixtures and configuration
├── requirements.txt         # Test dependencies
├── unit/                    # Unit tests for core functionality
│   ├── test_debian_parser.py
│   └── test_pkgx_load_urls.py
├── package_managers/        # Tests for package manager indexers
│   ├── crates/
│   │   ├── test_diff_deps.py
│   │   └── test_special_case.py
│   └── homebrew/
│       └── test_diff_dep.py
└── ranker/                  # Tests for ranking functionality
    ├── test_dedupe.py
    └── test_rx_graph.py
```

## Running Tests

### Prerequisites

Install test dependencies:

```bash
pip install -r tests/requirements.txt
```

### Running All Tests

To run all tests:

```bash
pytest tests/
```

### Running Specific Tests

Run tests for a specific module:

```bash
# Run all tests in a directory
pytest tests/package_managers/crates/

# Run a specific test file
pytest tests/unit/test_debian_parser.py

# Run a specific test class
pytest tests/unit/test_debian_parser.py::TestDebianParser

# Run a specific test method
pytest tests/unit/test_debian_parser.py::TestDebianParser::test_parse_package_data
```

### Running Tests by Marker

Tests are categorized with markers. To run tests for a specific category:

```bash
# Run only unit tests
pytest -m unit

# Run only parser tests
pytest -m parser

# Run only transformer tests
pytest -m transformer

# Run only ranker tests
pytest -m ranker

# Run all tests except slow ones
pytest -m "not slow"
```

### Verbose Output

For more detailed output:

```bash
pytest -v tests/

# Show captured print statements
pytest -s tests/

# Show local variables in tracebacks
pytest -l tests/
```

## Adding New Tests

### 1. Create a Test File

Test files should:
- Be placed in the appropriate directory based on what's being tested
- Follow the naming convention `test_*.py`
- Include a module docstring explaining what's being tested

Example:

```python
"""
Test the package parsing functionality for NewPackageManager.

This module tests the Parser class which extracts package information
from the package manager's data format.
"""

import pytest

from package_managers.newpm.parser import Parser
```

### 2. Use Fixtures for Common Setup

Instead of setUp/tearDown methods, use pytest fixtures:

```python
@pytest.fixture
def sample_package_data():
    """Provides sample package data for testing."""
    return {
        "name": "example-package",
        "version": "1.0.0",
        "dependencies": ["dep1", "dep2"],
    }

def test_parse_package(sample_package_data):
    """Test parsing a package with valid data."""
    parser = Parser()
    result = parser.parse(sample_package_data)
    assert result.name == "example-package"
```

### 3. Use Markers for Test Categories

Apply appropriate markers to your tests:

```python
@pytest.mark.parser
@pytest.mark.unit
class TestNewParser:
    """Test the new package manager parser."""
    
    def test_parse_valid_data(self):
        """Test parsing valid package data."""
        # test implementation
```

### 4. Mock External Dependencies

Use the fixtures from `conftest.py` or create specific mocks:

```python
def test_with_mocked_config(mock_config):
    """Test using the common mock_config fixture."""
    # mock_config is automatically injected from conftest.py
    transformer = Transformer(mock_config)
    # test implementation
```

### 5. Write Clear Assertions

Use clear, descriptive assertions:

```python
# Good
assert len(packages) == 3, "Should parse exactly 3 packages from the data"

# Less clear
assert len(packages) == 3
```

## Test Coverage

### Running Tests with Coverage

To generate a coverage report:

```bash
# Run with coverage and generate terminal report
pytest --cov=. --cov-report=term tests/

# Generate HTML coverage report
pytest --cov=. --cov-report=html tests/
# Open htmlcov/index.html in a browser

# Generate coverage for specific modules
pytest --cov=package_managers.crates --cov=ranker tests/

# Show missing lines in terminal
pytest --cov=. --cov-report=term-missing tests/
```

### Coverage by Docker Service

To check coverage for specific Docker services defined in `docker-compose.yml`:

```bash
# Coverage for crates indexer
pytest --cov=package_managers.crates --cov-report=term-missing tests/package_managers/crates/

# Coverage for homebrew indexer
pytest --cov=package_managers.homebrew --cov-report=term-missing tests/package_managers/homebrew/

# Coverage for debian indexer
pytest --cov=package_managers.debian --cov-report=term-missing tests/unit/test_debian_parser.py

# Coverage for pkgx indexer
pytest --cov=package_managers.pkgx --cov-report=term-missing tests/unit/test_pkgx_load_urls.py

# Coverage for ranker
pytest --cov=ranker --cov-report=term-missing tests/ranker/
```

### Setting Coverage Thresholds

To fail tests if coverage drops below a threshold:

```bash
pytest --cov=. --cov-fail-under=80 tests/
```

## Fixtures and Mocking

### Common Fixtures

The `conftest.py` file provides several reusable fixtures:

- **`mock_config`**: A mocked Config object with all sub-configurations
- **`mock_url_types`**: Mocked URL types (homepage, repository, etc.)
- **`mock_dependency_types`**: Mocked dependency types (runtime, build, dev, test)
- **`mock_package_managers`**: Mocked package manager configurations
- **`sample_package_data`**: Sample data for different package managers

### Using Fixtures

Fixtures are automatically injected into test functions:

```python
def test_example(mock_config, sample_package_data):
    """Example test using multiple fixtures."""
    # mock_config and sample_package_data are automatically available
    crates_data = sample_package_data["crates"]
    # test implementation
```

### Creating Test-Specific Fixtures

For test-specific setup, create local fixtures:

```python
@pytest.fixture
def special_cache():
    """Create a cache with specific test data."""
    return Cache(
        package_map={"test": Package(id=uuid4(), name="test")},
        url_map={},
        dependencies={},
    )

def test_with_special_cache(special_cache):
    """Test using the special cache."""
    # test implementation
```

## Test Markers

Available markers (defined in `conftest.py`):

- **`@pytest.mark.unit`**: Unit tests
- **`@pytest.mark.integration`**: Integration tests
- **`@pytest.mark.slow`**: Slow-running tests
- **`@pytest.mark.parser`**: Parser tests
- **`@pytest.mark.transformer`**: Transformer tests
- **`@pytest.mark.loader`**: Loader tests
- **`@pytest.mark.ranker`**: Ranker tests

To list all available markers:

```bash
pytest --markers
```

## Best Practices

1. **Test One Thing**: Each test should verify a single behavior
2. **Use Descriptive Names**: Test names should clearly indicate what they test
3. **Keep Tests Independent**: Tests should not depend on each other
4. **Use Fixtures**: Leverage fixtures for common setup instead of duplicating code
5. **Mock External Dependencies**: Don't make actual database or network calls
6. **Test Edge Cases**: Include tests for error conditions and edge cases
7. **Document Complex Tests**: Add docstrings explaining complex test scenarios

## Troubleshooting

### Common Issues

1. **Import Errors**: Ensure `PYTHONPATH` includes the project root:
   ```bash
   export PYTHONPATH=/workspace:$PYTHONPATH
   ```

2. **Missing Dependencies**: Install test requirements:
   ```bash
   pip install -r tests/requirements.txt
   ```

3. **Database Connection Errors**: Tests should not require `CHAI_DATABASE_URL`. If a test fails due to database issues, it likely needs better mocking.

### Debugging Tests

To debug a failing test:

```bash
# Drop into debugger on failure
pytest --pdb tests/failing_test.py

# Show local variables in traceback
pytest -l tests/failing_test.py

# Increase verbosity
pytest -vv tests/failing_test.py
```