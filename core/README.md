# Core Tools for CHAI Python Loaders

This directory contains a set of core tools and utilities to facilitate loading the CHAI
database with package manager data, using python helpers. These tools provide a common
foundation for fetching, transforming, and loading data from various package managers
into the database.

In general, the flow of an indexer is:

1. Fetch data from source
2. Fetch data from CHAI
3. Do a giant diff
4. Create new entries, updated entries for each package model in the db

The best example is [Homebrew's](../package_managers/homebrew/main.py).

## Key Components

### [Config](config.py)

Entrypoint for all loaders, generally has all the information needed for the pipeline
to start. Includes:

- Execution flags:
  - `FETCH` determines whether we request the data from source
  - `TEST` enables a test mode, to test specific portions of the pipeline
  - `NO_CACHE` to determine whether we save the intermediate pipeline files
- Package Manager flags
  - `pm_id` gets the package manager id from the db, that we'd run the pipeline for
  - `source` is the data source for that package manager. `SOURCES` defines the map.

The next 4 configuration classes retrieve the IDs for url types (homepage, documentation,
etc.), dependency types (build, runtime, etc.), user types (crates user, github user),
and all the package manager IDs as well.

### 2. [Database](db.py)

The DB class offers a set of methods for interacting with the database, including:

- Running queries to build a cache for the current state of the graph for a package
  manager
- Batching utilities
- Some load functions

### 3. [Fetcher](fetcher.py)

The Fetcher class provides functionality for downloading and extracting data from
package manager sources. It supports:

- Downloading tarball / GZIP / Git files
- Extracting contents to a specified directory
- Maintaining a "latest" symlink so we always know where to look

### 4. [Logger](logger.py)

A custom logging utility that provides consistent logging across all loaders.

### 5. [Models](models/__init__.py)

SQLAlchemy models representing the database schema, including:

- Package, Version, User, License, DependsOn, and other relevant tables

> [!NOTE]
>
> This is currently used to actually generate the migrations as well

### 6. [Scheduler](scheduler.py)

A scheduling utility that allows loaders to run at specified intervals.

### 7. [Transformer](transformer.py)

The Transformer class provides a base for creating package manager-specific transformers.
It includes:

- Methods for locating and reading input files
- Placeholder methods for transforming data into the required format

## Usage

To create a new loader for a package manager:

1. Create a new directory under `package_managers/` for your package manager.
1. Implement a fetcher that inherits from the base Fetcher, that is able to fetch
   the raw data from the package manager's source.
1. Implement a custom Transformer class that inherits from the base Transformer, that
   figures out how to map the raw data provided by the package managers into the data
   model described in the [models](models/__init__.py) module.
1. Load the cache for data currently in CHAI for that package manager
1. Implement a diff to compare them
1. Pass diff objects (lists of new / updated data points) to `db.ingest`
1. Orchestrate via a `main.py`.

Example usage can be found in the [crates](../package_managers/crates) loader.

# TODOs

- [ ] `Diff` currently has separate implementations for Homebrew and Crates, and could
      be centralized - open to help here!
