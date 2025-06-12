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

1. **Initialization**: The loader starts by initializing the configuration and database
   connection using `Config` and `CratesDB`.
2. **Fetching**: If the `FETCH` flag is set to true, the loader downloads the latest
   cargo data from the source using `TarballFetcher`. If needed, it saves to disk.
3. **Transformation**: The downloaded data is parsed and transformed using
   `CratesTransformer.parse()` into a format compatible with the CHAI database schema.
4. **Deletion**: The loader identifies crates that exist in the database
   but are no longer in the registry (crates.io allows deletion _sometimes_).
5. **Cache Building**: The loader builds a cache by setting the current graph and URLs
   from the database, then creates a `Cache` object for efficient diffing.
6. **Diff Process**: The loader performs a diff operation to categorize data into:
   - New packages vs updated packages
   - New URLs vs existing URLs
   - New package URLs vs updated package URLs
   - New dependencies vs removed dependencies
7. **Data Ingestion**: All categorized data is loaded into the database via a single
   `db.ingest()` call.

The main execution logic is in the `main` function in [main.py](main.py):

```python
def main(config: Config, db: CratesDB):
    logger = Logger("crates_main")
    logger.log("Starting crates_main")

    # fetch, write, transform
    if config.exec_config.fetch:
        fetcher = TarballFetcher(...)
        files = fetcher.fetch()
    if not config.exec_config.no_cache:
        fetcher.write(files)

    transformer = CratesTransformer(config)
    transformer.parse()

    # identify and handle deletions
    deletions = identify_deletions(transformer, db)
    if deletions:
        db.delete_packages_by_import_id(deletions)

    # build cache and diff
    db.set_current_graph()
    db.set_current_urls(crates_urls)
    cache = Cache(...)

    # perform diff and ingest
    diff = Diff(config, cache)
    # ... diff process ...
    db.ingest(new_packages, final_new_urls, new_package_urls,
              new_deps, removed_deps, updated_packages, updated_package_urls)
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

## TODOs

- [ ] `versions.csv` contains all the `published_by` ids, who are the users, who'd need to
      be loaded as well
- [ ] `versions.csv` also contains licenses
