# CHAI Data Migrations

Include a readme that talks about the alembic service that is provided by this
directory. Make sure you detail the functioning of the entrypoint
[script](./run_migrations.sh). An example would look like this:

## Steps

1. [Initialize](init-script.sql)

The initialization script creates the database `chai`, and loads it up with any
extensions that we'd need

2. [Load](load-values.sql)

The load script pre-populated some of the tables, with `enum`-like values - specifically
for `url_types`, `depends_on_types`, and `package_managers`

Also talk about the tool alembic, and including a `## Contributing` section that talks
about how to add data models. It could look something like this

## Contributing

- Currently, we use `alembic`
- Make a change in the [models](../core/models/__init__.py) file
- `alembic revision --autogenerate "<message>"`

Please, please review and make changes to your migrations!

Additionally, add a comment about how this is a starting point, and we're probably not
going to stick to `alembic`.
