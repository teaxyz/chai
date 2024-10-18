#!/bin/bash

set -exu

export SOURCE="https://formulae.brew.sh/api/formula.json"
export NOW=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
export JQ_DIR="package_managers/homebrew/jq"
mkdir -p data/homebrew/$NOW

# extract
curl -s $SOURCE > data/homebrew/$NOW/source.json

# make a symlink called latest, pointing to $NOW
ln -sfn $NOW data/homebrew/latest

# transform
for x in $JQ_DIR/*.jq; do
  echo $x
  filename=$(basename "$x" .jq)
  pkgx jq -f $x data/homebrew/latest/source.json > data/homebrew/latest/${filename}.json
  # | json2csv > data/homebrew/latest/${x%.jq}.csv
done

# load



# make it all csv
# jq -r '(map(keys) | add | unique) as $cols | map(. as $row | $cols | map($row[.])) as $rows | $cols, $rows[] | @csv'

# psql to load raw csv files