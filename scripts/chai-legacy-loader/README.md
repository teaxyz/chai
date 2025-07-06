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

   1. `psql $LEGACY_CHAI_DATABASE_URL -t -A -F',' -f sql/packages.sql -o /path/to/output.csv`
   1. Run `add_package_fields.py /file/from/step/1.csv /path/to/output package_manager_id`
      to enrich it with additional fields
   1. `psql $CHAI_DATABASE_URL -c "CREATE TABLE temp_import (LIKE packages);"`
   1. `psql $CHAI_DATABASE_URL -c "\COPY temp_import (id, derived_id, name, package_manager_id, import_id, created_at, updated_at) FROM '/path/to/csv/from/step/2' WITH (FORMAT csv, HEADER true, DELIMITER ',');"`
   1. `psql $CHAI_DATABASE_URL -c "INSERT INTO packages SELECT * FROM temp_import ON CONFLICT DO NOTHING;"`
   1. `psql $CHAI_DATABASE_URL -c "DROP TABLE temp_import;"`

3. Loading dependencies

With pkgx, just invoking the script from the root directory of chai

```bash
cd ../..
PYTHONPATH=. copy_dependencies_no_thread.py
```

4. Loading URLs

   1. Run [urls.sql](sql/urls.sql), which generates a csv
   1. Run `batch_insert_urls.py /path/to/step/1 -d` to insert the raw URLs, and get a
      dump of the loaded IDs and the URL
   1. Run `batch_insert_package_urls.py /path/to/step/1 --urls /path/to/step/2` to
      insert the package_url relationships. If no cache is provided, it'll try to read
      all loaded URLs and their IDs from the db (long)

```bash
pkgx psql -h localhost -U gardener -p 5430 temp_chai < dev_chai_fixed.sql
```
