# CHAI

CHAI is an attempt at an open-source data pipeline for package managers. The
goal is to have a pipeline that can use the data from any package manager and
provide a normalized data source for myriads of different use cases.

## Getting Started

Use [Docker](https://docker.com)

1. Run `docker compose build` to create the latest Docker images.
2. Then, run `docker compose up` to launch.

> [!NOTE]
>
> This will run CHAI with for all package managers. As an example crates by
> itself will take over an hour and consume >5GB storage.
>
> Currently, we support only two package managers:
>
> - crates
> - Homebrew
>
> You can run a single package manager by running
> `docker compose up -e ... <package_manager>`
>
> We are planning on supporting `NPM`, `PyPI`, and `rubygems` next.

### Arguments

Specify these eg. `docker compose -e FOO=bar up`:

- `FREQUENCY`: Sets how often (in hours) the pipeline should run.
- `TEST`: Runs the loader in test mode when set to true, skipping certain data insertions.
- `FETCH`: Determines whether to fetch new data from the source when set to true.
- `NO_CACHE`: When set to true, deletes temporary files after processing.

> [!NOTE]
> The flag `NO_CACHE` does not mean that files will not get downloaded to your local
> storage, just that we'll delete the files once we're done with them

These arguments are all configurable in the `docker-compose.yml` file.

### Docker Services Overview

1. `db`: [PostgreSQL] database for the reduced package data
2. `alembic`: handles migrations
3. `package_managers`: fetches and writes data for each package manager
4. `api`: a simple REST api for reading from the db

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

### start

Requires: build

```sh
docker compose up -d
```

### test

Env: TEST=true
Env: DEBUG=true

```sh
docker compose up
```

### full-test

Requires: build
Env: TEST=true
Env: DEBUG=true

```sh
docker compose up
```

### stop

```sh
docker compose down
```

### logs

```sh
docker compose logs
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

### db-list-packages

```sh
psql "postgresql://postgres:s3cr3t@localhost:5435/chai" -c "SELECT count(id) FROM packages;"
```

### db-list-history

```sh
psql "postgresql://postgres:s3cr3t@localhost:5435/chai" -c "SELECT * FROM load_history;"
```

### restart-api

Refreshes table knowledge from the db.

```sh
docker-compose restart api
```

### remove-orphans

```sh
docker compose down --remove-orphans
```

[PostgreSQL]: https://www.postgresql.org
[`pkgx`]: https://pkgx.sh
