# CHAI

This is an attempt at an open-source data pipeline for package managers. The goal is to
have a pipeline that can run on any package manager and provide a normalized data
source for different use cases.

1. db: postgres to store package specific data
1. alembic: for running migrations
1. pipeline: which fetches and writes data

## Requirements

- docker

> [!TIP]
>
> for local development, all the requirements are within the [pkgx yaml](pkgx.yaml) file

## Setup

1. Run `docker compose build` to create the latest Docker images.
2. Run `docker compose up` to launch.

### Arguments

- `PKG_MANAGER`: which package manager the pipeline will be run for. Currently, the
  supported values are:
  - `crates`
- `FREQUENCY`: how frequently **(in hours)** the pipeline will run. Defaults to `24`
- `FETCH`: whether the pipeline will fetch the data. Defaults to `true`
- `DEBUG`: whether the pipeline will run in debug mode. Defaults to `true`

These arguments are all configurable in the `docker-compose.yml` file.

## Hard Reset

If at all you need to do a hard reset, here's the steps

1. `rm -rf data`: removes all the data the fetcher is putting
2. `docker system prune -a -f --volumes`: removes **everything** docker-related

> [!WARNING]
>
> step 4 deletes all your docker stuff...be careful

<!-- this is handled now that alembic/psycopg2 are in pkgx -->
<!--
## Alembic Alternatives

- sqlx command line tool to manage migrations, alongside models for sqlx in rust
- vapor's migrations are written in swift
-->

## Usage

Our goal is to build a data schema that looks like this:

![db/CHAI_ERD.png](db/CHAI_ERD.png)

Our specific application extracts the dependency graph understand what are critical
pieces of the open-source graph. there are many other potential use cases for this data:

- license compatibility checker
- developer publications
- package popularity
- dependency analysis vulnerability tool (requires translating semver)

<!-- TODO: add these to the examples folder-->

### license compatibility checker

> [!WARNING]
>
> it's probably better to start with a global list of licenses and then map each
> version's to the global list...but this isn't part of v1

```sql
SELECT DISTINCT
   p.name,
   l.name AS license,
   dep.name AS dependency,
   dep_l.name AS dependency_license
FROM packages p
JOIN versions v ON p.id = v.package_id
JOIN dependencies d ON v.id = d.version_id
JOIN packages dep ON d.dependency_id = dep.id
JOIN licenses l ON v.license_id = l.id
JOIN versions dep_v ON dep.id = dep_v.package_id
JOIN licenses dep_l ON dep_v.license_id = dep_l.id
```

### package popularity

```sql
SELECT p.name, SUM(v.downloads) as total_downloads
FROM packages p
JOIN versions v ON p.id = v.package_id
GROUP BY p.name
ORDER BY total_downloads DESC
LIMIT 10;
```

### developer publications

```sql
SELECT u.username, p.name, COUNT(uv.id) as publications
FROM users u
JOIN user_versions uv ON u.id = uv.user_id
JOIN versions v ON uv.version_id = v.id
JOIN packages p ON v.package_id = p.id
GROUP BY u.username, p.name
ORDER BY p.name;
```

## FAQs / common issues

1. The database url is `postgresql://postgres:s3cr3t@localhost:5435/chai`, and is used
   as `CHAI_DATABASE_URL` in the environment.

## tasks

These are tasks that can be run using xcfile.dev. if you have pkgx, just run `dev` to
inject into your environment. if you don't...go get it.

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
