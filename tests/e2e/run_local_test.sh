#!/usr/bin/env bash
set -euo pipefail

# Local end-to-end pipeline test runner
# This script tests the pipelines locally before running in CI

echo "🧪 Running local end-to-end pipeline tests..."

# Set test environment variables
export CHAI_DATABASE_URL="postgresql://postgres:s3cr3t@localhost:5435/chai"
export TEST=true
export NO_CACHE=true
export ENABLE_SCHEDULER=false
export FETCH=false

# Ensure results directory exists
mkdir -p tests/e2e/results

# Check if postgres is running
if ! docker compose ps db | grep -q "running"; then
    echo "❌ Database is not running. Starting it..."
    docker compose up -d db
    echo "⏳ Waiting for database to be ready..."
    sleep 10
fi

# Run alembic migrations
echo "📦 Running database migrations..."
docker compose run --rm alembic

# Create test expected data if it doesn't exist
if [ ! -f "tests/e2e/expected/homebrew_expected.json" ]; then
    echo "📝 Creating default expected data..."
    mkdir -p tests/e2e/expected
    python tests/e2e/validate_homebrew.py || true  # This will create the default expected file
fi

# Test Homebrew pipeline
echo "🍺 Testing Homebrew pipeline..."
docker compose -f docker-compose.yml -f docker-compose.test.yml run --rm homebrew

# Validate Homebrew results
echo "✅ Validating Homebrew results..."
python tests/e2e/validate_homebrew.py

# Run database state validation
echo "🔍 Validating database state..."
python tests/e2e/validate_database_state.py

# Check results
if [ -f "tests/e2e/results/validation_summary.json" ]; then
    echo "📊 Test Summary:"
    cat tests/e2e/results/validation_summary.json | jq '.'
    
    # Check if all tests passed
    if cat tests/e2e/results/validation_summary.json | jq -e '.overall_success == true' > /dev/null; then
        echo "✅ All tests passed!"
        exit 0
    else
        echo "❌ Some tests failed. Check the results above."
        exit 1
    fi
else
    echo "❌ No validation summary found. Tests may have failed to run."
    exit 1
fi