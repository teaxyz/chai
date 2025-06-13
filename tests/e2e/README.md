# End-to-End Pipeline Testing

This directory contains the end-to-end testing framework for CHAI package manager pipelines.

## Overview

The E2E tests validate that each package manager pipeline:
1. Correctly loads test data from fixtures
2. Processes the data through the full pipeline
3. Stores results in the database with proper relationships
4. Maintains cache integrity and deduplication

## Key Components

### Test Fixtures (`fixtures/`)
- Contains sample data for each package manager
- Located in `/data/{package_manager}/` inside containers
- Example: `fixtures/homebrew/formulae.json`

### Expected Results (`expected/`)
- JSON files defining expected outcomes
- Auto-generated on first run with sensible defaults
- Can be customized for specific test scenarios

### Validators (`validate_*.py`)
- Package-specific validators extending `BaseValidator`
- Check packages, dependencies, and URLs
- Perform integrity and cache effectiveness checks

### Results (`results/`)
- JSON output from each validator
- `validation_summary.json` aggregates all results

## Running Tests

### Locally
```bash
# Run the local test script
./tests/e2e/run_local_test.sh
```

### In CI
Tests run automatically via GitHub Actions on:
- Push to main
- Pull requests
- Manual workflow dispatch

### Individual Pipeline Test
```bash
# Set environment variables
export CHAI_DATABASE_URL="postgresql://postgres:s3cr3t@localhost:5432/chai_test"
export TEST=true
export NO_CACHE=true
export ENABLE_SCHEDULER=false
export FETCH=false

# Run specific pipeline
docker compose -f docker-compose.yml -f docker-compose.test.yml run --rm homebrew

# Validate results
python tests/e2e/validate_homebrew.py
```

## Adding New Tests

1. Create test fixtures in `fixtures/{package_manager}/`
2. Implement validator in `validate_{package_manager}.py`:
   ```python
   class NewValidator(BaseValidator):
       def load_expected_data(self):
           # Define expected results
       
       def validate_packages(self, expected):
           # Package-specific validation
       
       def validate_dependencies(self, expected):
           # Dependency validation
       
       def validate_urls(self, expected):
           # URL validation
   ```

3. Update pipeline fetcher to support test mode:
   ```python
   if self.test:
       # Load from /data/{package_manager}/fixture.json
   ```

4. Add to CI workflow in `.github/workflows/e2e-pipeline-test.yml`

## What These Tests Catch

1. **Cache Structure Issues**: Like the `url_cache` → `url_map` refactoring
2. **Data Type Mismatches**: URLKey vs tuple inconsistencies
3. **Referential Integrity**: Orphaned records, missing foreign keys
4. **Deduplication Failures**: Duplicate URLs that should be cached
5. **Pipeline Logic Errors**: Missing dependencies, incorrect mappings

## Test Data Structure

### Fixtures
Minimal but representative data covering:
- Basic packages with all URL types
- Various dependency relationships
- Edge cases (missing URLs, circular deps)

### Expected Results
```json
{
  "packages": ["pkg1", "pkg2"],
  "dependencies": {
    "pkg1": ["dep1", "dep2"]
  },
  "urls": {
    "pkg1": {
      "homepage": "https://...",
      "source": "https://..."
    }
  }
}
```

## Debugging Failed Tests

1. Check `results/{package_manager}_validation.json` for specific errors
2. Review `results/validation_summary.json` for overall status
3. Database queries for investigation:
   ```sql
   -- Check for orphaned records
   SELECT * FROM package_urls pu
   LEFT JOIN packages p ON pu.package_id = p.id
   WHERE p.id IS NULL;
   
   -- Check cache effectiveness
   SELECT COUNT(DISTINCT url) as unique_urls,
          COUNT(*) as total_references
   FROM urls u
   JOIN package_urls pu ON u.id = pu.url_id;
   ```

## Future Enhancements

- [ ] Performance benchmarking
- [ ] Memory usage tracking
- [ ] Concurrent pipeline testing
- [ ] Regression test suite from production data
- [ ] Automated fixture generation from API samples