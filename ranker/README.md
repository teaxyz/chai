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

1.  **Fetch Existing State:** Retrieves all current canonical URLs and their associated 
IDs from the `canons` table.
2.  **Determine Latest URLs:** Fetches all packages and their associated homepage URLs 
from the database, identifying the *most recent* URL for each package.
3.  **Identify New Canons:** Compares the latest URLs with the existing `canons` table 
to find URLs that do not yet have a canonical entry.
4.  **Prepare Data:**
    *   Creates `Canon` objects for newly identified canonical URLs.
    *   Creates `CanonPackage` mapping objects representing the desired state: linking 
    each package ID to the canonical ID corresponding to its latest valid homepage URL.
5.  **Load/Update Database:**
    *   Loads the new `Canon` objects using `INSERT ... ON CONFLICT (url) DO NOTHING`. This adds new canons without duplicating existing ones.
    *   Loads the `CanonPackage` mappings using 
    `INSERT ... ON CONFLICT (package_id) DO UPDATE SET canon_id = ...`. This inserts 
    mappings for new packages and updates the `canon_id` for existing packages if their 
    canonical URL has changed.

This process is idempotent, meaning running it multiple times converges to the same 
correct state based on the latest available package URL data.

## Ranking

- [ ] Add a description here

## Usage

### With pkgx

```bash
chmod +x main.py
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