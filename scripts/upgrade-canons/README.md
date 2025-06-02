# upgrade canons

Create canonical URL entries in the URLs table for non-standardized canons in CHAI

## How it works

1. Identify the current set of canons
2. Identify the current Homepage URLs
3. Iterate through canons, and find canons.url that are standardized. If they aren't:
   1. Run through permalint
   2. Skip if it's already canonicalized and in urls, OR we've already tried to
      canonicalize it
   3. Create a new URL entry
   4. Map the Old URL ID to the New URL ID (for the next step)
4. Query package links for non-standardized URLs from step 3
5. Recreate package url links for new URLs

## Requirements

1. pkgx
2. connection to CHAI_DATABASE_URL as an environment variable

## Usage

```bash
$ chmod +x scripts/upgrade-canons/main.py
$ scripts/upgrade-canons/main.py --homepage-id <homepage_url_id_from_chai>
```

If you include the `--dry-run` flag, then it'll show you what it's going to insert,
rather than insert it

## Output

```
Found 1171064 existing canons
Found 1206172 existing URLs
dd94a2e5-7fce-4379-8d6b-eed07592edd6: http://  "files": [ is malformed: Invalid IPv6 URL
  ⭐️ Populated 1090357 canonical URLs
  ⭐️ Skipped 44222 URLs that were already canonicalized
  ⭐️ Skipped 35569 URLs that were already added
Found 1356405 existing package URLs
----------------------------------------------------------------------------------------------------
Going to insert:
  1090357 URLs
  1357116 PackageURLs
----------------------------------------------------------------------------------------------------
Inserted 1090357 rows into urls
Inserted 1357116 rows into package_urls
```
