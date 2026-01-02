# Upgrade Canons Scripts

Collection of scripts for managing canonical URLs and Canon IDs in CHAI database.

## Scripts Overview

| Script                     | Purpose                                                            | Usage                                                         | Sample Output                               |
| -------------------------- | ------------------------------------------------------------------ | ------------------------------------------------------------- | ------------------------------------------- |
| `main.py`                  | Creates canonical URL entries for non-standardized homepage URLs   | `./main.py --homepage-id <uuid> [--dry-run]`                  | `✅ Inserted 45678 URLs, 52341 PackageURLs` |
| `registered_projects.py`   | Updates Canon IDs for registered projects to restore old canon IDs | `cat canon_ids.txt \| ./registered_projects.py [--dry-run]`   | `✅ Success: 150`<br>`❌ Failure: 25`       |
| `create_deleted_canons.py` | Creates canons for registered projects that were deleted           | `./create_deleted_canons.py --csv-file input.csv [--dry-run]` | `✅ Success: 75`<br>`❌ Failure: 12`        |

## Requirements

- pkgx (or uv)
- CHAI_DATABASE_URL environment variable
- Python dependencies: `psycopg2==2.9.10`, `permalint==0.1.14`

## Common Options

- `--dry-run`: Show what would be done without making changes
- Input failures are written to CSV files for review

## Database Schema Dependencies

Scripts interact with these tables:

- `urls`, `url_types`, `package_urls`
- `canons`, `canon_packages`, `canon_packages_old`
- `tea_ranks`, `packages`
