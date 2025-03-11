# CHAI Legacy Data Loader

Tools for loading legacy CHAI data into the current CHAI database framework.

## Overview

This project provides scripts to efficiently transfer data from the legacy CHAI database into the current CHAI schema. It handles large volumes of data through batched processing to prevent memory issues.

## Loader Scripts

- `package_loader.py`: Loads package data from the legacy database to the current schema

## Usage

1. Set up environment variables (or use defaults):

```bash
export LEGACY_DB_HOST=localhost
export LEGACY_DB_NAME=chai_legacy
export LEGACY_DB_USER=postgres
export LEGACY_DB_PASSWORD=postgres
export LEGACY_DB_PORT=5432
export CHAI_DB_URL=postgresql://postgres:postgres@localhost:5432/chai
```

2. Run the package loader:

```bash
python package_loader.py
```

## Data Loading Order

Based on database relationships, the loaders should be run in this order:

1. Packages
2. Versions (requires package IDs)
3. URLs and Package URLs
4. Dependencies (requires version IDs and package IDs)

## Development

### Requirements

- Python 3.6+
- `psycopg2`
- `sqlalchemy`

Install dependencies:

```bash
pip install psycopg2-binary sqlalchemy
```

### Adding New Loaders

When adding loaders for additional tables:

1. Create SQL files in the `sql/` directory to extract data from legacy tables
2. Follow the pattern in existing loaders for efficient batch processing
3. Maintain proper relationships by loading tables in the correct order
