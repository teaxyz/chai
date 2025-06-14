# upgrade canons

Create canonical URL entries in the URLs table for non-standardized homepage URLs in CHAI

## How it works

1. **Get all homepage data**: Query the database to get all existing homepage URLs and
   map each package to its list of homepage URL strings
2. **Analyze packages needing canonicalization**: For each package:
   - Skip if the package already has at least one canonical URL
     (using `permalint.is_canonical_url`)
   - Generate the canonical URL for the package's URLs (using `permalint.normalize_url`)
   - Skip if the canonical URL already exists in the database
   - Skip if we're already planning to create this canonical URL for another package
   - Mark this package as needing a canonical URL created
3. **Create objects**: For each package that needs canonicalization:
   - Create a new `URL` object with the canonical URL
   - Create a new `PackageURL` object linking the package to the new canonical URL
4. **Ingest to database**: Insert the new URLs and PackageURLs into the database

## Key Logic

The script implements smart deduplication:

- **Avoids duplicates**: Won't create a canonical URL if it already exists in the database
- **Handles conflicts**: If multiple packages would generate the same canonical URL, only the first one processed gets it
- **Preserves existing**: Packages that already have canonical URLs are left untouched
- **Memory efficient**: Loads all data upfront to avoid database round-trips and constraint violations

## Requirements

1. pkgx (or uv)
2. Connection to CHAI_DATABASE_URL as an environment variable
3. Python dependencies: `psycopg2==2.9.10`, `permalint==0.1.14`

## Usage

```bash
$ chmod +x scripts/upgrade-canons/main.py
$ scripts/upgrade-canons/main.py --homepage-id <homepage_url_type_id_from_chai>
```

If you include the `--dry-run` flag, then it'll show you what it's going to insert
without actually inserting it.

## Example Output

```
Starting main: 2024-01-15 10:30:45.123456
Found 1171064 homepages
Found 1206172 packages with URLs
----------------------------------------------------------------------------------------------------
Going to insert:
  45678 URLs
  52341 PackageURLs
----------------------------------------------------------------------------------------------------
Inserted 45678 rows into urls
Inserted 52341 rows into package_urls
```

## Testing

```bash
$ pkgx uv run pytest tests/scripts/upgrade_canons/
```

Key test scenarios covered:

- Packages that need canonical URLs created
- Packages that already have canonical URLs (skipped)
- Canonical URLs that already exist in database (skipped)
- Multiple packages generating the same canonical URL (deduplication)
