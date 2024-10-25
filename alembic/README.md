# CHAI Data Migrations

This directory contains the Alembic configuration and migration scripts for managing the
database schema of the CHAI project. Alembic is used to handle database migrations,
allowing for version control of our database schema.

### About Alembic

Alembic is a database migration tool for SQLAlchemy. It allows us to:

- Track changes to our database schema over time
- Apply and revert these changes in a controlled manner
- Generate migration scripts automatically based on model changes

> [!NOTE]
> It's important to note that while `alembic` serves our current needs, it may not be
> our long-term solution. As the CHAI project evolves, we might explore other database
> migration tools or strategies that better fit our growing requirements. We're open to
> reassessing our approach to schema management as needed.

## Entrypoint

The main entrypoint for running migrations is the
[run migrations script](run_migrations.sh). This script orchestrates the initialization
and migration process.

## Steps

1. [Initialize](init-script.sql)

The initialization script creates the database `chai`, and loads it up with any
extensions that we'd need, so we've got a clean slate for our db structures.

2. [Load](load-values.sql)

The load script pre-populated some of the tables, with `enum`-like values - specifically
for:

- `url_types`: defines different types of URLs (e.g., source, homepage, documentation)
- `depends_on_types`: defines different types of dependencies (e.g., runtime,
  development)
- `sources` and `package_managers`: defines different package managers (e.g., npm, pypi)

3. Run Alembic Migrations

After initialization and loading initial data, the script runs Alembic migrations to apply any pending database schema changes.

## Contributing

To contirbute to the database schema:

1. Make a change in the [models](../core/models/__init__.py) file
2. Generate a new migration script: `alembic revision --autogenerate "Description"`
3. Review the generated migration script in the [versions](versions/) directory. The
   auto-generation is powerful but not perfect, please review the script carefully.
4. Test the migration by running `alembic upgrade head`.
