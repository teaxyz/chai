# CHAI

inspiration [here](https://github.com/vbelz/Data-pipeline-twitter)

this is an attempt at an open-source data pipeline, from which we can build our app that
ranks open-source projects. all pieces for this are managed by `docker-compose`. there
are 3 services to it:

1. db: postgres to store package specific data
1. alembic: for running migrations
1. pipeline: which fetches and writes data

## Setup

1. Run `docker compose build` to create the latest Docker images.
2. Run `docker compose up` to launch.

> [!IMPORTANT]
>
> the `PKG_MANAGER` argument denotes which package manager the pipeline will be run for
> it is configurable, and defaults to `crates`

> [!TIP]
>
> to force it, `docker-compose up --force-recreate --build`

## Hard Reset

if at all you need to do a hard reset, here's the steps

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

our goal is to build a data schema that looks like this:

<!-- ![db/CHAI_ERD.png](db/CHAI_ERD.png) -->

our specific application extracts the dependency graph understand what are critical
pieces of the open-source graph. there are many other potential use cases for this data:

- impacts of a package update (is it compatible with other packages)
- finding name squats
- finding the superpower maintainers
- number of versions / releases per package, and a release cadence
- ...

### top packages by number of versions

```sql
SELECT p."name", count(v."version") AS total_versions
FROM packages p
JOIN versions v ON p.id = v.package_id
GROUP BY p."name"
ORDER BY total_versions DESC;
```

### some creative ways to detect name squats

```sql
SELECT left(p."name", 5), count(1) as number_of_packages
FROM packages p
GROUP BY left(p."name", 5)
ORDER BY number_of_packages DESC;
```

### url types available

```sql
-- #TODO
```

### what platform is most used for code hosting?

```sql
-- #TODO
```

### packages where we're missing repo urls

```sql
-- #TODO
```

## FAQs / common issues

1. the database url is `postgresql://postgres:s3cr3t@localhost:5435/chai`, and is used
   as `CHAI_DATABASE_URL` in the environment.
1. there are two bash scripts use by the alembic and pipeline services, and you'd need
   to run the following to ensure they can be executed:
   ```sh
   chmod +x alembic/run_migrations.sh
   chmod +x src/run_pipeline.sh
   ```

## tasks

these are tasks that can be run using xcfile.dev. if you have pkgx, just run `dev` to
inject into your environment. if you don't...go get it.

### reset

```sh
rm -rf db/data data .venv
```

### build

```sh
docker-compose build
```

### start

Requires: build
Env: PKG_MANAGER=${PKG_MANAGER:-crates}
Env: CHAI_DATABASE_URL=${CHAI_DATABASE_URL:-"postgresql://postgres:s3cr3t@localhost:5435/chai"}

```sh
docker compose up -d
```

### test

Requires: build
Env: PKG_MANAGER=${PKG_MANAGER:-crates}
Env: CHAI_DATABASE_URL=${CHAI_DATABASE_URL:-"postgresql://postgres:s3cr3t@localhost:5435/chai"}
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
