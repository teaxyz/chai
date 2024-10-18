#!/bin/bash

set -exu
NOW=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
mkdir -p "$DATA_DIR"/"$NOW"

# extract
curl -s "$SOURCE" > "$DATA_DIR"/"$NOW"/source.json

# make a symlink called latest, pointing to $NOW
ln -sfn "$NOW" "$DATA_DIR"/latest

# transform
echo "$JQ_DIR"
for x in "$JQ_DIR"/*.jq; do
  filename=$(basename "$x" .jq)
  jq -f "$x" "$DATA_DIR"/latest/source.json > "$DATA_DIR"/latest/"${filename}".json
  # | json2csv > data/homebrew/latest/${x%.jq}.csv
done

# load



# make it all csv
# jq -r '(map(keys) | add | unique) as $cols | map(. as $row | $cols | map($row[.])) as $rows | $cols, $rows[] | @csv'

# psql to load raw csv files