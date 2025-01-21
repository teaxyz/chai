# Core Tools for CHAI Python Loaders

This directory contains a set of core tools and utilities to facilitate loading the CHAI database with package manager data using Python helpers. These tools provide a common foundation for fetching, transforming, and loading data from various package managers into the database.

## Key Components

### 1. [Config](config.py)

Config always runs first and serves as the entry point for all loaders. It includes:

- **Execution flags:**
  - `FETCH`: Determines whether to request data from the source.
  - `TEST`: Enables a test mode for specific portions of the pipeline.
  - `NO_CACHE`: Specifies whether intermediate pipeline files should be saved.

- **Package Manager flags:**
  - `pm_id`: Retrieves the package manager ID from the database for which the pipeline will be executed.
  - `source`: Defines the data source for the package manager. `SOURCES` contains the mapping.

The next three configuration classes retrieve the IDs for:
- URL types (homepage, documentation, etc.).
- Dependency types (build, runtime, etc.).
- User types (Crates user, GitHub user).

### 2. [Database](db.py)

The `DB` class provides a set of methods for interacting with the database, including:

- Inserting and selecting data for packages, versions, users, dependencies, and more.
- Caching mechanisms to improve performance.
- Batch processing for efficient data insertion.

### 3. [Fetcher](fetcher.py)

The `Fetcher` class provides functionality for downloading and extracting data from package manager sources. It supports:

- Downloading tarball files.
- Extracting contents to a specified directory.
- Maintaining a "latest" symlink for easy access to the most recent data.

### 4. [Logger](logger.py)

A custom logging utility that ensures consistent logging across all loaders.

### 5. [Models](models/__init__.py)

SQLAlchemy models representing the database schema, including:

- `Package`, `Version`, `User`, `License`, `DependsOn`, and other relevant tables.

> **Note:**  
> These models are also used to generate database migrations.

### 6. [Scheduler](scheduler.py)

A scheduling utility that enables loaders to run at specified intervals.

### 7. [Transformer](transformer.py)

The `Transformer` class provides a base for creating package manager-specific transformers. It includes:

- Methods for locating and reading input files.
- Placeholder methods for transforming data into the required format.

## Usage

To create a new loader for a package manager:

1. Create a new directory under `package_managers/` for your package manager.
2. Implement a fetcher that inherits from the base `Fetcher` and fetches raw data from the package manager's source.
3. Implement a custom `Transformer` class that inherits from the base `Transformer` and maps raw data to the data model described in the [models](models/__init__.py) module.
4. Create a main script that utilizes the core components (`Config`, `DB`, `Fetcher`, `Transformer`, `Scheduler`) to fetch, transform, and load data.

An example implementation can be found in the [Crates](../package_managers/crates) loader.
