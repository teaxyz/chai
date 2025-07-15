#!/bin/bash

# CHAI API Test Runner Script
# This script provides an easy way to run the test suite with different options

set -e  # Exit on any error

echo "🧪 CHAI API Test Runner"
echo "======================="

# Check if cargo is available
if ! command -v cargo &> /dev/null; then
    echo "❌ Cargo not found. Trying with pkgx..."
    if command -v pkgx &> /dev/null; then
        CARGO_CMD="pkgx cargo"
    else
        echo "❌ Neither cargo nor pkgx found. Please install Rust/Cargo or pkgx."
        exit 1
    fi
else
    CARGO_CMD="cargo"
fi

# Function to run tests with different options
run_tests() {
    local test_type="$1"
    local extra_args="$2"
    
    echo "🚀 Running $test_type tests..."
    echo "Command: $CARGO_CMD test $extra_args"
    echo "---"
    
    if $CARGO_CMD test $extra_args; then
        echo "✅ $test_type tests passed!"
    else
        echo "❌ $test_type tests failed!"
        exit 1
    fi
    echo ""
}

# Parse command line arguments
case "${1:-all}" in
    "all")
        echo "Running all tests..."
        run_tests "All" ""
        ;;
    "unit")
        echo "Running unit tests only..."
        run_tests "Unit" "--lib"
        ;;
    "integration")
        echo "Running integration tests only..."
        run_tests "Integration" "--test integration_tests"
        ;;
    "utils")
        echo "Running utils tests only..."
        run_tests "Utils" "utils::tests"
        ;;
    "handlers")
        echo "Running handlers tests only..."
        run_tests "Handlers" "handlers::tests"
        ;;
    "db")
        echo "Running database tests only..."
        run_tests "Database" "db::tests"
        ;;
    "verbose")
        echo "Running all tests with verbose output..."
        run_tests "All (verbose)" "-- --nocapture"
        ;;
    "help")
        echo "Usage: $0 [test_type]"
        echo ""
        echo "Available test types:"
        echo "  all          - Run all tests (default)"
        echo "  unit         - Run unit tests only"
        echo "  integration  - Run integration tests only"
        echo "  utils        - Run utils module tests only"
        echo "  handlers     - Run handlers module tests only"
        echo "  db           - Run database module tests only"
        echo "  verbose      - Run all tests with verbose output"
        echo "  help         - Show this help message"
        echo ""
        echo "Examples:"
        echo "  $0                    # Run all tests"
        echo "  $0 unit               # Run unit tests only"
        echo "  $0 utils              # Run utils tests only"
        echo "  $0 verbose            # Run all tests with output"
        exit 0
        ;;
    *)
        echo "❌ Unknown test type: $1"
        echo "Run '$0 help' for usage information."
        exit 1
        ;;
esac

echo "🎉 Test run completed successfully!"