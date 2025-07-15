# CHAI API

CHAI API is a REST API service for accessing the CHAI database, which contains package manager data.

## Features

- List all tables in the database
- Fetch paginated data from any table
- Heartbeat endpoint for health checks

## Requirements

- Rust 1.67 or later
- PostgreSQL database

## API Endpoints

### Health Check

```
GET /heartbeat
```

Returns the health status of the API and database connection.

**Response (Success)**
```txt
OK - Database connection is healthy
```
**Response (Failure - Database query failed):**
```txt
Database query failed
```
**Response (Failure - Database connection failed):**
```txt
Failed to get database connection
```

### List Tables

```
GET /tables
```

Returns a paginated list of all available tables in the database.

**Query Parameters**
- `page` (optional): Page number (default: 1)
- `limit` (optional): Number of items per page (default: 200)

**Response**
```json
{
    "data": [
        "alembic_version",
        "sources",
        "package_managers",
        "url_types",
        "urls",
        "users",
        "load_history",
        "packages",
        "package_urls",
        "user_packages",
        "licenses",
        "versions",
        "dependencies",
        "depends_on_types",
        "user_versions"
    ],
    "limit": 200,
    "page": 1,
    "total_count": 15,
    "total_pages": 1
}
```

### Get Table Data

```
GET /{table}
```

Returns paginated data from the specified table.

**Path Parameters**
- `table`: Name of the table to query (see available tables in List Tables response)

**Query Parameters**
- `page` (optional): Page number (default: 1)
- `limit` (optional): Number of items per page (default: 200)

**Response**
```json
{
    "table": "packages",
    "total_count": 166459,
    "page": 1,
    "limit": 2,
    "total_pages": 83230,
    "columns": [
        ...
    ],
    "data": [
        {
            "created_at": "2024-12-27 08:04:03.991832",
            "derived_id": "...",
            "id": "...",
            "import_id": "...",
            "name": "...",
            "package_manager_id": "...",
            "readme": "...",
            "updated_at": "2024-12-27 08:04:03.991832"
        },
        ...
    ]
}
```

### Get Table Row By ID

```
GET /{table}/{id}
```

Returns a specific row from the table by its UUID.

**Path Parameters**
- `table`: Name of the table to query
- `id`: UUID of the row to fetch

**Response**
```json
{
    "created_at": "2024-12-27 08:04:03.991832",
    "derived_id": "...",
    "id": "...",
    "import_id": "...",
    "name": "...",
    "package_manager_id": "...",
    "readme": "...",
    "updated_at": "2024-12-27 08:04:03.991832"
}
```

## Available Tables

The database contains the following tables:

| Table Name | Description |
| --- | --- |
| alembic_version | Store the current version of alembic |
| dependencies | Package dependencies |
| depends_on_types | Types of package dependencies |
| licenses | Package licenses |
| load_history | Load history |
| package_managers | Package manager information |
| package_urls | Relationship of packages to URLs |
| packages | Package metadata |
| sources | Package manager sources (homebrew, crates, etc.) |
| url_types | Types of URLs (homepage, repository, etc.) |
| urls | Actual URLs |
| user_packages | User-package relationships |
| user_versions | User-version relationships |
| users | User (package owner) information |
| versions | Package versions |

By default, the API will be available at `http://localhost:8080`.

## Testing

The CHAI API includes a comprehensive test suite that covers utility functions, handlers, and basic integration tests. The tests are designed to run without requiring a live database connection.

### Quick Start

```bash
# Run all tests
cargo test

# Or use the test runner script
./run_tests.sh

# Run specific test categories
./run_tests.sh unit        # Unit tests only
./run_tests.sh integration # Integration tests only
./run_tests.sh utils       # Utils module tests only
./run_tests.sh verbose     # All tests with output
```

### Test Coverage

The test suite includes:

- **Utils Tests** - Complete coverage of data transformation functions
- **Handlers Tests** - Input validation and response formatting
- **Database Tests** - Configuration parsing and URL handling
- **Integration Tests** - Basic endpoint testing with mock data
- **App State Tests** - Application structure validation

### Test Documentation

For detailed information about the test suite, including:
- Test structure and organization
- Running specific tests
- Adding new tests
- Debugging test failures
- CI/CD integration

See: [`TEST_SUITE.md`](TEST_SUITE.md)

### Test Files

- `src/utils.rs` - Unit tests for utility functions
- `src/handlers.rs` - Unit tests for handler logic
- `src/db.rs` - Unit tests for database configuration
- `src/app_state.rs` - Unit tests for application state
- `tests/integration_tests.rs` - Integration tests for API endpoints
- `run_tests.sh` - Test runner script
- `TEST_SUITE.md` - Comprehensive test documentation
