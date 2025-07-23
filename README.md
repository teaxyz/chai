# CHAI
[![CI](https://github.com/teaxyz/chai/actions/workflows/ci.yml/badge.svg)](https://github.com/teaxyz/chai/actions/workflows/ci.yml)

CHAI is an attempt at an open-source data pipeline for package managers. The
goal is to have a pipeline that can use the data from any package manager and
provide a normalized data source for myriads of different use cases.

## Getting Started

Use [Docker](https://docker.com)

1. Install Docker
2. Clone the chai repository (https://docs.github.com/en/repositories/creating-and-managing-repositories/cloning-a-repository)
3. Using a terminal, navigate to the cloned repository directory
4. Run `docker compose build` to create the latest Docker images
5. Then, run `docker compose up` to launch.

> [!NOTE]
>
> This will run CHAI for all package managers. As an example crates by
> itself will take over an hour and consume >5GB storage.
>
> Currently, we support:
>
> - crates
> - Homebrew
> - Debian
> - pkgx
>
> You can run a single package manager by running
> `docker compose up -e ... <package_manager>`
>
> We are planning on supporting `NPM`, `PyPI`, and `rubygems` next.

### Arguments

Specify these eg. `FOO=bar docker compose up`:

- `ENABLE_SCHEDULER`: When true, the pipeline runs on a schedule set by `FREQUENCY`.
- `FREQUENCY`: Sets how often (in hours) the pipeline should run.
- `TEST`: Useful for running in a test code
  insertions.
- `FETCH`: Determines whether to fetch new data or use whatever was saved locally.
- `NO_CACHE`: When true, deletes temporary files after processing.

> [!NOTE]
> The flag `NO_CACHE` does not mean that files will not get downloaded to your local
> storage (specifically, the ./data directory). It only means that we'll
> delete these temporary files from ./data once we're done processing them.
> If `FETCH` is false, the pipeline looks for source data in the cache, so this
> will fail if you run `NO_CACHE` first, and `FETCH` false second.

These arguments are all configurable in the `docker-compose.yml` file.

### Docker Services Overview

1. `db`: [PostgreSQL] database for the reduced package data
2. `alembic`: handles migrations
3. `package_managers`: fetches and writes data for each package manager
4. `api`: a simple REST API for reading from the db
5. `ranker`: deduplicates and ranks the packages

### Hard Reset

Stuff happens. Start over:

`rm -rf ./data`: removes all the data the fetcher is putting.

<!-- this is handled now that alembic/psycopg2 are in pkgx -->
<!--
## Alembic Alternatives

- sqlx command line tool to manage migrations, alongside models for sqlx in rust
- vapor's migrations are written in swift
-->

## Goals

Our goal is to build a data schema that looks like this:

![db/CHAI_ERD.png](db/CHAI_ERD.png)

You can read more about specific data models in the dbs [readme](db/README.md)

Our specific application extracts the dependency graph understand what are
critical pieces of the open-source graph. We also built a simple example that displays
[sbom-metadata](examples/sbom-meta) for your repository.

There are many other potential use cases for this data:

- License compatibility checker
- Developer publications
- Package popularity
- Dependency analysis vulnerability tool (requires translating semver)

> [!TIP]
> Help us add the above to the examples folder.

## FAQs / Common Issues

1. The database url is `postgresql://postgres:s3cr3t@localhost:5435/chai`, and
   is used as `CHAI_DATABASE_URL` in the environment. `psql CHAI_DATABASE_URL`
   will connect you to the database.
2. If you're orchestrating via docker, swap `localhost` for `host.docker.internal`

## Troubleshooting / Common Errors

### Docker Compose fails to start db
- Make sure port 5435 is not already used by another service.
- If you get a permission error on the data folder, run your terminal as administrator or fix the permissions for the `./data` folder.

### Migration error (alembic)
- Ensure you have run `docker compose build --no-cache db alembic` before `docker compose up alembic`.
- Check that the `CHAI_DATABASE_URL` environment variable is set correctly.

### Database connection refused
- Make sure the `db` service is healthy before starting other services.
- If you are using Docker Desktop on Windows/Mac, use `host.docker.internal` as the database host.

### Out of disk space
- Fetching package manager data can consume more than 5GB. Ensure you have enough storage before running the pipeline.

### Permission denied when deleting ./data
- Run `rm -rf ./data` with administrator/root privileges.

### Service stuck or not responding
- Check logs with `docker compose logs <service>` to see detailed errors.
- Restart the service with `docker compose restart <service>`.

## Managing Dependencies

We use [`uv`](https://astral.sh/uv) to manage dependencies (and sometimes execution).
All dependencies are listed in [`pyproject.toml`](./pyproject.toml), under the
`dependency-groups` header. Each group helps us classify the service we're adding a
dependency for. For example, if we're adding a new dependency for all the indexers:

```bash
uv add --group indexer requests

# use the --all-groups flag to sync your venv for all dependencies
uv sync --all-groups
uv pip compile --group indexer -o core/requirements.txt
```

The last step writes the updated dependencies to a requirements.txt file, which is
crucial for the Docker containers executing the individual services to build correctly.
Each indexer shares the same set of dependencies, and that requirement file is
**generated by uv**, and maintained in [core/requirements.txt](core/requirements.txt)

> [!IMPORTANT]
> DO NOT UPDATE ANY `requirements.txt` FILES DIRECTLY
> `uv` provides a way to generate that automatically, based on the pyproject.toml
>
> Have an idea on a better way to do this? Open to input...

## Deployment

```sh
export CHAI_DATABASE_URL=postgresql://<user>:<pw>@host.docker.internal:<port>/chai
export PGPASSWORD=<pw>
docker compose up alembic
```

## Tasks

These are tasks that can be run using [xcfile.dev]. If you use `pkgx`, typing
`dev` loads the environment. Alternatively, run them manually.

### reset

```sh
rm -rf db/data data .venv
```

### build

```sh
docker compose build
```

### start-all

Requires: build

```sh
docker compose up -d
```

### stop

```sh
docker compose down
```

### logs

```sh
docker compose logs
```

### db-start

Runs migrations and starts up the database

```sh
docker compose build --no-cache db alembic
docker compose up alembic -d
```

### db-reset

Requires: stop

```sh
rm -rf db/data
```

### db-generate-migration

Inputs: MIGRATION_NAME
Env: CHAI_DATABASE_URL=postgresql://postgres:s3cr3t@localhost:5435/chai

```sh
cd alembic
alembic revision --autogenerate -m "$MIGRATION_NAME"
```

### db-upgrade

Env: CHAI_DATABASE_URL=postgresql://postgres:s3cr3t@localhost:5435/chai

```sh
cd alembic
alembic upgrade head
```

### db-downgrade

Inputs: STEP
Env: CHAI_DATABASE_URL=postgresql://postgres:s3cr3t@localhost:5435/chai

```sh
cd alembic
alembic downgrade -$STEP
```

### db

```sh
psql "postgresql://postgres:s3cr3t@localhost:5435/chai"
```

### restart-api

Refreshes table knowledge from the db.

```sh
docker compose restart api
```

### remove-orphans

```sh
docker compose down --remove-orphans
```

### start-service

Inputs: SERVICE
Env: CHAI_DATABASE_URL=postgresql://postgres:s3cr3t@host.docker.internal:5435/chai

```sh
docker compose up $SERVICE --build
```

### check

Inputs: FOLDER
Environment: FOLDER=.

```sh
pkgx +python@3.13 ty check $FOLDER
```

[PostgreSQL]: https://www.postgresql.org
[`pkgx`]: https://pkgx.sh
