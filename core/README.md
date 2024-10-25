# Core Tools for CHAI Python Loaders

This directory contains a set of core tools and utilities to facilitate loading the CHAI
database with packaage manager data, using python helpers. These tools provide a common
foundation for fetching, transforming, and loading data from various package managers
into the database.

## Key Components

### 1. [Config](config.py)

Config always runs first, and is the entrypoint for all loaders. It includes;

- Execution flags:
  - `FETCH` determines whether we request the data from source
  - `TEST` enables a test mode, to test specific portions of the pipeline
  - `NO_CACHE` to determine whether we save the intermediate pipeline files
- Package Manager flags
  - `pm_id` gets the package manager id from the db, that we'd run the pipeline for
  - `source` is the data source for that package manager. `SOURCES` defines the map.

The next 3 configuration classes retrieve the IDs for url types (homepage, documentation,
etc.), dependency types (build, runtime, etc.) and user types (crates user, github user)

### 2. [Database](db.py)

The DB class offers a set of methods for interacting with the database, including:

- Inserting and selecting data for packages, versions, users, dependencies, and more
- Caching mechanisms to improve performance
- Batch processing capabilities for efficient data insertion

### 3. [Fetcher](fetcher.py)

The Fetcher class provides functionality for downloading and extracting data from
package manager sources. It supports:

- Downloading tarball files
- Extracting contents to a specified directory
- Maintaining a "latest" symlink so we always know where to look

### 4. [Logger](logger.py)

A custom logging utility that provides consistent logging across all loaders.

### 5. [Models](models/**init**.py)

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
   model described in the [models](models/**init**.py) module.
1. Create a main script that utilizes the core components (Config, DB, Fetcher,
   Transformer, Scheduler) to fetch, transform, and load data.

Example usage can be found in the [crates](../package_managers/crates) loader.
