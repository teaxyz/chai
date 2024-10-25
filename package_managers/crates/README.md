# crates

The crates service uses the database dump provided by crates.io and coerces their data
model into CHAI's. It's containerized using Docker for easy deployment and consistency.
It's also written in `python` as a first draft, and uses a lot of the
[core tools](../../core/).

## Getting Started

To just run the crates service, you can use the following commands:

```bash
docker compose build crates
docker compose run crates
```

## Execution Steps

The crates loader goes through the following steps when executed:

1. Initialization: The loader starts by initializing the configuration and database
   connection.
2. Fetching: If the `FETCH` flag is set to true, the loader downloads the latest crates
   data from the configured source.
3. Transformation: The downloaded data is transformed into a format compatible with the
   CHAI database schema.
4. Loading: The transformed data is loaded into the database. This includes:
   - Packages
   - Users
   - User Packages
   - URLs
   - Package URLs
   - Versions
   - Dependencies
5. Cleanup: After successful loading, temporary files are cleaned up if the `NO_CACHE` flag is set.

The main execution logic is in the `run_pipeline` function in [main.py](main.py).

```python
def run_pipeline(db: DB, config: Config) -> None:
    fetcher = fetch(config)
    transformer = CratesTransformer(config.url_types, config.user_types)
    load(db, transformer, config)
    fetcher.cleanup(config)

    coda = (
        "validate by running "
        + '`psql "postgresql://postgres:s3cr3t@localhost:5435/chai" '
        + '-c "SELECT * FROM load_history;"`'
    )
    logger.log(coda)
```

### Configuration Flags

The crates loader supports several configuration flags:

- `DEBUG`: Enables debug logging when set to true.
- `TEST`: Runs the loader in test mode when set to true, skipping certain data insertions.
- `FETCH`: Determines whether to fetch new data from the source when set to true.
- `FREQUENCY`: Sets how often (in hours) the pipeline should run.
- `NO_CACHE`: When set to true, deletes temporary files after processing.

These flags can be set in the `docker-compose.yml` file:

```yaml
crates:
  build:
    context: .
    dockerfile: ./package_managers/crates/Dockerfile
  environment:
    - CHAI_DATABASE_URL=postgresql://postgres:s3cr3t@db:5432/chai
    - PYTHONPATH=/
    - DEBUG=${DEBUG:-false}
    - TEST=${TEST:-false}
    - FETCH=${FETCH:-true}
    - FREQUENCY=${FREQUENCY:-24}
    - NO_CACHE=${NO_CACHE:-false}
```

## Notes

- We're reopening the same files multiple times, which is not efficient.
  - `versions.csv` contains all the `published_by` ids
  - `crates.csv` contains all the `urls`
- The cache logic in the database client is super complicated, and needs some better
  explanation...it does work though.
- Licenses are non-standardized.
- Warnings on missing users are because `gh_login` in the source data is non-unique.
