# CHAI API Test Suite

This document describes the comprehensive test suite for the CHAI API, which provides focused unit tests for the utility functions and basic integration tests for the API endpoints.

## Test Structure

The test suite is organized into several components:

### 1. Unit Tests (in source files)

#### `src/utils.rs` Tests
- **`convert_optional_to_json`** - Tests conversion of `Result<Option<T>, E>` to JSON values
  - Tests with various data types: `i32`, `String`, `bool`, `f64`, `UUID`, `DateTime`, `NaiveDate`, etc.
  - Tests error handling and `None` value handling
  - Tests array conversions

- **`get_column_names`** - Tests column name extraction from database rows
  - Tests with empty row sets
  - Tests column name extraction logic

- **`Pagination`** - Tests pagination logic
  - Tests default value handling (page=1, limit=200)
  - Tests custom pagination parameters
  - Tests limit clamping (min=1, max=1000)
  - Tests page clamping (min=1, max=total_pages)
  - Tests offset calculation
  - Tests total pages calculation

#### `src/handlers.rs` Tests
- **`check_table_exists`** - Tests table validation logic
  - Tests with valid tables
  - Tests with invalid tables
  - Tests with empty table lists
  - Tests case sensitivity

- **`PaginationParams`** - Tests query parameter deserialization
  - Tests default values
  - Tests custom values
  - Tests invalid parameter handling
  - Tests negative values

- **`PaginatedResponse`** - Tests response serialization
  - Tests JSON serialization
  - Tests response structure

#### `src/app_state.rs` Tests
- **`AppState`** - Tests application state structure
  - Tests struct field types
  - Tests memory layout

#### `src/db.rs` Tests
- **Database URL parsing** - Tests PostgreSQL connection string parsing
  - Tests valid URLs with all components
  - Tests URLs without passwords
  - Tests URLs with default ports
  - Tests URLs with special characters
  - Tests invalid URL handling

- **Database configuration** - Tests database config creation
  - Tests config field assignment
  - Tests path stripping logic

### 2. Integration Tests (`tests/integration_tests.rs`)

#### API Endpoint Tests
- **`/tables` endpoint** - Tests table listing functionality
  - Tests basic endpoint response
  - Tests response structure validation
  - Tests pagination functionality
  - Tests invalid pagination parameters
  - Tests empty table scenarios

#### Application Setup Tests
- **App configuration** - Tests basic application setup
  - Tests service configuration
  - Tests endpoint routing

## Running the Tests

### Prerequisites
1. Install Rust and Cargo
2. Ensure you have the required dependencies

### Running All Tests
```bash
# Using cargo (standard)
cargo test

# Using pkgx (if available)
pkgx cargo test

# Run only unit tests
cargo test --lib

# Run only integration tests
cargo test --test integration_tests

# Run tests with output
cargo test -- --nocapture

# Run specific test
cargo test test_pagination_new_with_defaults
```

### Running Tests by Module
```bash
# Run utils tests only
cargo test utils::tests

# Run handlers tests only
cargo test handlers::tests

# Run database tests only
cargo test db::tests
```

## Test Coverage

The test suite covers:

### ✅ Well Tested
- **Utility Functions** - Complete coverage of data transformation logic
- **Pagination Logic** - Comprehensive testing of pagination calculations
- **Input Validation** - Table existence checking, parameter validation
- **Error Handling** - Testing of error conditions and edge cases
- **Data Serialization** - JSON conversion and response formatting

### ⚠️ Limited Testing
- **Database Operations** - Basic configuration testing only (no live DB required)
- **HTTP Endpoints** - Basic endpoint testing with mock data
- **Authentication** - Not applicable (no auth in current implementation)

### ❌ Not Tested
- **Full Database Integration** - Requires live database connection
- **Complex Endpoint Logic** - Full CRUD operations with real data
- **Error Recovery** - Database connection failure scenarios
- **Performance** - Load testing and performance benchmarks

## Test Philosophy

The test suite follows these principles:

1. **Isolation** - Tests don't depend on external services (database, network)
2. **Focus** - Tests target specific functionality without complex setup
3. **Clarity** - Tests are readable and document expected behavior
4. **Maintainability** - Tests are easy to update as code changes

## Adding New Tests

When adding new functionality, follow these guidelines:

### For Utility Functions
```rust
#[test]
fn test_new_utility_function() {
    // Test normal case
    let result = new_utility_function(valid_input);
    assert_eq!(result, expected_output);
    
    // Test edge cases
    let result = new_utility_function(edge_case_input);
    assert_eq!(result, expected_edge_output);
    
    // Test error cases
    let result = new_utility_function(invalid_input);
    assert!(result.is_err());
}
```

### For Handler Functions
```rust
#[test]
fn test_new_handler_logic() {
    // Test the pure logic parts that don't require database
    let result = handler_validation_logic(test_input);
    assert!(result.is_ok());
}
```

### For Integration Tests
```rust
#[actix_web::test]
async fn test_new_endpoint() {
    let app_state = create_mock_app_state();
    let app = test::init_service(
        App::new()
            .app_data(app_state)
            .service(new_endpoint)
    ).await;
    
    let req = test::TestRequest::get()
        .uri("/new-endpoint")
        .to_request();
    
    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success());
}
```

## Debugging Tests

### Common Issues
1. **Test Dependencies** - Ensure all required crates are in `[dev-dependencies]`
2. **Module Visibility** - Make sure functions are properly exposed for testing
3. **Async Tests** - Use `#[actix_web::test]` for async integration tests
4. **Mock Data** - Ensure mock data matches expected format

### Running Tests in Debug Mode
```bash
# Run with debug output
RUST_LOG=debug cargo test

# Run single test with output
cargo test test_name -- --nocapture --exact

# Run tests with backtrace
RUST_BACKTRACE=1 cargo test
```

## CI/CD Integration

The test suite is designed to run in CI/CD environments:

```yaml
# Example GitHub Actions workflow
- name: Run tests
  run: cargo test --verbose
  
- name: Run tests with coverage
  run: cargo test --all-features --no-fail-fast
```

## Performance Considerations

- Tests are designed to run quickly without external dependencies
- Mock data is used to avoid database overhead
- Tests can run in parallel safely
- Integration tests use minimal setup

## Contributing

When contributing to the test suite:

1. Add tests for new functionality
2. Update existing tests when changing behavior
3. Follow the existing test patterns and naming conventions
4. Document any complex test setups
5. Ensure tests are deterministic and don't rely on timing