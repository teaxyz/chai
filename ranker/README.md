# ranker

generates a deduplicated graph across all CHAI package managers by URL, and publishes a
tea_rank

## Requirements

1. [pkgx](pkgx.sh)
2. [uv](astral.sh/uv)

## Deduplication (`dedupe.py`)

`dedupe.py` handles the deduplication of packages based on their homepage URLs. It
ensures that packages sharing the same canonical homepage URL are grouped together.

**Process:**

1.  **Fetch Existing State:** Retrieves all current canonical homepage URLs, their
    associated packages from the `canons`, `canon_packages`, and `package_urls` tables
2.  **Determine Latest URLs:** Identifies the most recent URL
3.  **Diff:** Identify new canons, new canon_packages, canon_packages to update
4.  **Ingest:** Create new canons and new links if necessary, update existing ones

This process is idempotent, meaning running it multiple times converges to the same
correct state based on the latest available package URL data.

### Getting started

1. You need `CHAI_DATABASE_URL` setup, and the CHAI db running
2. With pkgx:

   ```bash
   chmod +x ranker/dedupe.py
   PYTHONPATH=. LOAD=0 ranker/dedupe.py
   ```

   You can toggle LOAD to do a dry-run, where it will tell you what it's about to do
   without loading any information

## Ranking

- [ ] Add a description here

## Usage

### With pkgx

```bash
chmod +x dedupe.py
./main.py
```

### Without pkgx

```bash
uv run main.py
```

## Docker

This service can be run inside a Docker container. The container assumes that the `core`
library is available and that the `CHAI_DATABASE_URL` environment variable is set to
point to the database.

**Building the Image:**

From the root of the `chai-oss` repository:

```bash
docker build -t chai-ranker -f ranker/Dockerfile .
```

**Running the Container:**

Make sure to provide the database connection string via the `CHAI_DATABASE_URL`
environment variable:

```bash
docker run --rm -e CHAI_DATABASE_URL=postgresql://postgres:s3cr3t@localhost:5435/chai chai-ranker
```

The container will execute `dedupe.py` followed by `main.py` and exit with code 0 on
success or a non-zero code on failure.
