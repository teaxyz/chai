# PyPI

The PyPI service processes package data from the Python Package Index (PyPI) and transforms it into CHAI's normalized format. It uses PyPI's JSON data dumps for efficient bulk processing.

## Getting Started

To run the PyPI service, use the following commands:

```bash
docker compose build pypi
docker compose run pypi
```

## Execution Steps

The PyPI loader follows these steps:

1. Initialization: Sets up configuration and database connection
2. Fetching: Downloads the latest PyPI JSON data dump if `FETCH` is true
3. Transformation: Converts PyPI's JSON format into CHAI's schema
4. Loading: Inserts transformed data into the database:
   - Packages
   - Users
   - User Packages
   - URLs
   - Package URLs
   - Versions
   - Dependencies
5. Cleanup: Removes temporary files if `NO_CACHE` is true

The main execution logic is in the `run_pipeline` function in `main.py`.
