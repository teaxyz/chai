#!/bin/bash

set -exu
NOW=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
mkdir -p "$DATA_DIR"/"$NOW"

if [ "$FETCH" = true ]; then
  # extract
  curl -s "$SOURCE" > "$DATA_DIR"/"$NOW"/source.json

  # make a symlink called latest, pointing to $NOW
  ln -sfn "$NOW" "$DATA_DIR"/latest

  # transform
  echo "$JQ_DIR"
  for x in "$JQ_DIR"/*.jq; do
    filename=$(basename "$x" .jq)
    # first jq line uses the formulas defined in the jq folder to get the fields we need
    # second jq line transforms the json into csv so we can use sed to clean it up
    jq -f "$x" "$DATA_DIR"/latest/source.json \
      | jq -r '
          (map(keys) | add | unique) as $cols |
          map(. as $row | $cols | map($row[.])) as $rows |
          $cols, $rows[] | @csv
      ' \
      > "$DATA_DIR"/latest/"${filename}".csv
  done
fi

# load
# TODO: put in a sed folder
# sed -f "$SED_DIR/packages.sed" "$DATA_DIR/latest/packages.csv" > "$DATA_DIR/latest/package_inserts.sql"
sed '1d;s/"\([^"]*\)","\([^"]*\)","\([^"]*\)",*/INSERT INTO packages (derived_id, import_id, name) VALUES ('\''\1'\'', '\''\2'\'', '\''\3'\'');/' "$DATA_DIR"/latest/packages.csv > "$DATA_DIR"/latest/package_inserts.sql
psql "$CHAI_DATABASE_URL" -f "$DATA_DIR"/latest/package_inserts.sql

# sed -f "$SED_DIR"/urls.sed "$DATA_DIR"/latest/urls.csv \
#   > "$DATA_DIR"/latest/url_inserts.sql
# psql "$CHAI_DATABASE_URL" -f "$DATA_DIR"/latest/url_inserts.sql

# loading package_urls is a bit more complicated, because we need to do it in batches
# and we need to get the ids from the package and url tables
# sed -f "$SED_DIR"/package_url.sed "$DATA_DIR"/latest/package_url.csv > "$DATA_DIR"/latest/package_url_inserts.sql
# psql "$CHAI_DATABASE_URL" -f "$DATA_DIR"/latest/package_url_inserts.sql
