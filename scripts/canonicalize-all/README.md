# Canonicalize All URLs Script

This script ensures that every package in the CHAI database has exactly one canonicalized URL for each URL type where it has at least one URL entry.

## Purpose

The script addresses URL consistency by:

1. **Identifying packages with URLs**: Scans all packages and their associated URLs across all URL types (homepage, repository, documentation, source)
2. **Checking canonicalization**: Uses `permalint.is_canonical_url()` to verify if existing URLs are already canonical
3. **Creating canonical URLs**: Uses `permalint.normalize_url()` to create canonical versions when needed
4. **Efficient loading**: Batches URL and PackageURL inserts for optimal database performance
5. **Avoiding duplicates**: Checks existing URLs to prevent constraint violations

## Usage

### Basic execution with dry run (recommended first step)

```bash
cd scripts/canonicalize-all
./main.py --dry-run
```

### Execute canonicalization

```bash
cd scripts/canonicalize-all
./main.py
```

## Requirements

- `CHAI_DATABASE_URL` environment variable must be set
- Database connection with read/write access
- Required Python packages:
  - `psycopg2==2.9.10`
  - `permalint==0.1.11`

## Process Flow

1. **Discovery**: Retrieves all URL types and package-URL relationships
2. **Analysis**: For each package and URL type combination:
   - Checks if a canonical URL already exists
   - If not, normalizes the first valid URL found
   - Handles malformed URLs gracefully
3. **Preparation**: Collects all URLs and PackageURLs that need to be created
4. **Execution**: Efficiently inserts new records using batch operations

## Output Statistics

The script provides detailed statistics including:

- Packages processed
- URL types processed
- Canonical URLs already existing
- Canonical URLs created
- Malformed URLs skipped
- Total inserts performed

## Safety Features

- **Dry run mode**: Preview all changes before execution
- **Duplicate prevention**: Checks existing URLs to avoid constraint violations
- **Transaction safety**: Uses database transactions for consistency
- **Error handling**: Gracefully handles malformed URLs and database errors

## Example Output

```
Found 4 URL types: ['documentation', 'homepage', 'repository', 'source']
Found 1234 packages with URLs
Processing package abc123... for URL type 'homepage' (3 URLs)
  ✓ Already has canonical URL: https://example.com
...

====================================================================================================
CANONICALIZATION SUMMARY
====================================================================================================
Packages processed: 1234
URL types processed: 2468
Canonical URLs already existed: 800
Canonical URLs to be created: 400
Normalized URLs already existed: 34
Malformed URLs skipped: 12
Total URLs to add: 400
Total PackageURLs to add: 434
====================================================================================================
```

## Related Scripts

- `scripts/upgrade-canons/`: Similar functionality but focused specifically on homepage URLs from canons table
