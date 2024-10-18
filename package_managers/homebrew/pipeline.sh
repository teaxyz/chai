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
  # first jq line uses the formulas defined in the jq folder to get the fields we need
  # second jq line transforms the json into csv
  jq -f "$x" "$DATA_DIR"/latest/source.json \
    | jq -r '
        (map(keys) | add | unique) as $cols |
        map(. as $row | $cols | map($row[.])) as $rows |
        $cols, $rows[] | @csv
    ' \
    > "$DATA_DIR"/latest/"${filename}".csv
done

# load



# make it all csv
# 
# > "$DATA_DIR"/latest/"${filename}".json

# psql to load raw csv files