#!/bin/bash

# make the data directory
mkdir -p data/{crates,pkgx,homebrew,npm,pypi,rubys}

# run the pipeline
python -u /src/run_scheduler.py
