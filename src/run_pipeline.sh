#!/bin/bash

# make directory structure
# working_dir is /app
mkdir -p data/{crates,pkgx,homebrew,npm,pypi,rubys}

python -u src/pipeline/main.py crates
