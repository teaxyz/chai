# CHAI Legacy Data Loader

Tools for loading legacy CHAI data into the current CHAI database framework.

> [!NOTE]
> This can only be executed if you have access to the Legacy CHAI database. If not,
> you can ignore everything inside this folder.

## Requirements

- pkgx.sh

## Overview

This is a set of utility python scripts to efficiently transfer data from the legacy CHAI
database into the current CHAI schema.

## Loader Scripts

- `add_package_fields.py`: enriches package data dumps from Legacy CHAI with fields
  required by CHAI
- `copy_dependencies_no_thread.py`: fetches dependency data from `public.sources` for a
  given package manager and uses psycopg2's `copy_expert` function to load it in
  batches into CHAI
- `add_urls.py`: add urls and package_urls relationships from Legacy CHAI

## Usage

1. Set up environment variables (or use defaults):

```bash
export LEGACY_CHAI_DATABASE_URL=credentials_from_itn
export CHAI_DATABASE_URL=postgresql://postgres:postgres@localhost:5435/chai
```

2. Loading packages

   1. Run [packages.sql](sql/packages.sql), which generates a csv
   1. Run `add_package_fields.py` to enrich it with additional fields
   1. `psql $CHAI_DATABASE_URL -c CREATE TABLE temp_import (LIKE packages)`
   1. `psql $CHAI_DATABASE_URL -c "\COPY temp_import (derived_id, name, import_id, id, package_manager_id, created_at, updated_at) FROM '/path/to/csv' WITH (FORMAT csv, HEADER true, DELIMITER ',')"`
   1. `psql $CHAI_DATABASE_URL -c "INSERT INTO packages SELECT * FROM temp_import ON CONFLICT DO NOTHING";`
   1. `psql $CHAI_DATABASE_URL -c "DROP TABLE temp_import"`

3. Loading dependencies

With pkgx, just invoking the script from the root directory of chai

```bash
cd ../..
PYTHONPATH=. copy_dependencies_no_thread.py
```

Or, if you have the legacy data already loaded locally...

```bash
psql "$LEGACY_CHAI_DATABASE_URL" -v package_manager='npm' -c "\copy (SELECT s.start_id, s.end_id, '81392e40-b4f2-4c06-9cd8-4fabff61e75e'::uuid AS dependency_type_id, NULL AS semver_range FROM public.sources s JOIN public.projects p ON s.start_id = p.id WHERE 'npm' = ANY(p.package_managers)) TO STDOUT" | psql "$CHAI_DATABASE_URL" -c "\copy legacy_dependencies (package_id, dependency_id, dependency_type_id, semver_range) FROM STDIN"
```

4. Loading URLs

   1. Run [urls.sql](sql/urls.sql), which generates a csv
   1. Run `add_urls.py path/to/csv` to load the data into CHAI
