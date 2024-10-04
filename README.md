# CHAI

inspiration [here](https://github.com/vbelz/Data-pipeline-twitter)

this is an attempt at an open-source data pipeline, from which we can build our app that
ranks open-source projects. all pieces for this are managed by `docker-compose`. there
are 3 services to it:

1. db: postgres to store package specific data
1. alembic: for running migrations
1. pipeline: which fetches and writes data

## Setup

first run `mkdir -p data/{crates,pkgx,homebrew,npm,pypi,rubys}`, to setup the data
directory where the fetchers will store the data.

then, running `docker compose up` will setup the db and run the pipeline. a successful
run will look something like this:

```
db-1       | 2024-09-23 18:33:31.199 UTC [1] LOG:  listening on IPv4 address "0.0.0.0", port 5432
db-1       | 2024-09-23 18:33:31.199 UTC [1] LOG:  listening on IPv6 address "::", port 5432
db-1       | 2024-09-23 18:33:31.202 UTC [1] LOG:  listening on Unix socket "/var/run/postgresql/.s.PGSQL.5432"
db-1       | 2024-09-23 18:33:31.230 UTC [30] LOG:  database system was shut down at 2024-09-23 18:04:05 UTC
db-1       | 2024-09-23 18:33:31.242 UTC [1] LOG:  database system is ready to accept connections
alembic-1  | db:5432 - accepting connections
alembic-1  | INFO  [alembic.runtime.migration] Context impl PostgresqlImpl.
alembic-1  | INFO  [alembic.runtime.migration] Will assume transactional DDL.
alembic-1  | db currently at 0db06140525f (head)
alembic-1  | INFO  [alembic.runtime.migration] Context impl PostgresqlImpl.
alembic-1  | INFO  [alembic.runtime.migration] Will assume transactional DDL.
alembic-1  | migrations run
alembic-1 exited with code 0
alembic-1   | postgresql://postgres:s3cr3t@db:5432/chai
alembic-1   | s3cr3t
pipeline-1  | 0.01: [crates_orchestrator]: [DEBUG]: logging is working
pipeline-1  | 0.01: [main_pipeline]: [DEBUG]: logging is working
pipeline-1  | 0.01: [DB]: [DEBUG]: logging is working
pipeline-1  | 0.03: [DB]: [DEBUG]: created engine
pipeline-1  | 0.03: [DB]: [DEBUG]: created session
pipeline-1  | 0.03: [DB]: [DEBUG]: connected to postgresql://postgres:s3cr3t@db:5432/chai
pipeline-1  | 0.03: [crates_orchestrator]: fetching crates packages
pipeline-1  | 0.03: [crates_fetcher]: [DEBUG]: logging is working
pipeline-1  | 0.03: [crates_fetcher]: [DEBUG]: adding package manager crates
```

> [!IMPORTANT]
>
> the `PKG_MANAGER` argument denotes which package manager the pipeline will be run for
> it is configurable, and defaults to `crates`

> [!TIP]
>
> to force it, `docker-compose up --force-recreate --build`

## Hard Reset

if at all you need to do a hard reset, here's the steps

1. `rm -rf db/data`: removes all the data that was loaded into the db
1. `rm -rf .venv`: if you created a virtual environment for local dev, this removes it
1. `rm -rf data`: removes all the data the fetcher is putting
1. `docker system prune -a -f --volumes`: removes **everything** docker-related

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

### setup

```sh
mkdir -p data/{crates,pkgx,homebrew,npm,pypi,rubys}
```

### local-dev

```sh
uv venv
cd src
uv pip install -r requirements.txt
```

### chai-start

Requires: setup
Inputs: FORCE
Env: FORCE=${FORCE:-not-force}
Env: PKG_MANAGER=${PKG_MANAGER:-crates}

```sh
if [ "$FORCE" = "force" ]; then
    docker-compose up --force-recreate --build -d
else
    docker-compose up -d
fi
export CHAI_DATABASE_URL="postgresql://postgres:s3cr3t@localhost:5435/chai"
```

### chai-stop

```sh
docker-compose down
```

### db-reset

Requires: chai-stop

```sh
rm -rf db/data
```

### db-logs

```sh
docker-compose logs db
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
