# Homebrew

The Homebrew service uses Homebrew's JSON API Documentation to build the Homebrew data
model. It's lightweight -- written in shell scripts, `jq`, and `psql` -- and
containerized using Docker

# Getting Started

To just run the Homebrew service, you can use the following commands:

```bash
docker compose build homebrew
docker compose run homebrew
```

## Pipeline Overview

The Homebrew pipeline consists of two main scripts:

- `pipeline.sh`: Responsible for fetching, transforming, and loading Homebrew package
  data.
- `schedule.sh`: Handles the scheduling and execution of the pipeline script.

> [!NOTE]
> The key aspect of `pipeline.sh` to note is how it prepares the sql statements - since
> our data model is completely normalized, we need to retrieve the IDs for each data
> model when loading our "edge" data.
>
> For example, in the `user_packages` table, we need to know the `user_id` and
> `package_id` for each record, which happens via a sub-select on each row. It sounds
> awful, but Homebrew's data is pretty small, so we're not asking the database to do
> much.

### [`schedule.sh`](schedule.sh)

The schedule.sh script sets up and manages the cron job for running the pipeline:

- Creates a cron job based on the `FREQUENCY` environment variable. Defaults to 24 hrs.
- Runs the pipeline immediately upon startup.
- Starts the cron daemon and tails the log file.

### [`jq` files](jq/)

The jq files in the [`jq/`](jq/) directory are responsible for transforming the raw
Homebrew JSON data into SQL statements for insertion into the database. Each file
corresponds to a specific table or relationship in the database.

To edit the jq files:

- Navigate to the [`jq/`](jq/) directory.
- Open the desired jq file in a text editor.
- Modify the jq queries as needed.

> [!NOTE]
> You can comment using `#` in the jq files!

Key jq files and their purposes:

- [`packages.jq`](jq/packages.jq): Transforms package data.
- [`urls.jq`](jq/urls.jq): Extracts and formats URL information.
- [`versions.jq`](jq/versions.jq): Handles version data (currently assumes latest version).
- [`package_url.jq`](jq/package_url.jq): Maps packages to their URLs.
- [`dependencies.jq`](jq/dependencies.jq): Processes dependency information.

## Notes

- Homebrew's dependencies are not just restricted to the `{build,test,...}_dependencies`
  fields listed in the JSON APIs...it also uses some system level packages denoted in
  `uses_from_macos`, and `variations` (for linux). The pipeline currently does consider
  these dependencies.
- Homebrew's JSON API and formula.rb files do not specify all the versions available for
  a package. It does provide the `stable` and `head` versions, which are pulled in
  [`versions.jq`](jq/versions.jq).
- Versioned formulae (like `python`, `postgresql`) are ones where the Homebrew package
  specifies a version. The pipeline considers these packages individual packages,
  and so creates new records in the `packages` table.
- The data source for Homebrew does not retrieve the analytics information that is
  available via the individual JSON API endpoints for each package.
