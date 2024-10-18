#!/bin/bash

set -exu

# get the ID for Homebrew from our database
HOMEBREW_ID=$(psql "$CHAI_DATABASE_URL" -f homebrew_id.sql -v "ON_ERROR_STOP=1" -tA)

# fail if HOMEBREW_ID is empty
if [ -z "$HOMEBREW_ID" ]; then
    echo "Error: Failed to retrieve Homebrew ID from the database."
    exit 1
fi

# homebrew provides `source` and `homepage` url types - let's create them ahead of time
psql "$CHAI_DATABASE_URL" -f create_url_types.sql

# if you've already pulled the Homebrew data, you can `export FETCH=false` to skip the
# download, and just work off the latest symlink
# Note that this only works if the volumes are mounted
if [ "$FETCH" = true ]; then
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
    # first jq line uses the formulas defined in the jq folder for each data model
    # second jq line transforms the json into csv so we can use sed to prep psql stmts
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
# TODO: loop?

# packages
# pass HOMEBREW_ID to sed to replace the @@HOMEBREW_ID@@ placeholder
sed \
  -f "$SED_DIR/packages.sed" "$DATA_DIR/latest/packages.csv" | \
  sed "s/@@HOMEBREW_ID@@/$HOMEBREW_ID/" \
  > "$DATA_DIR/latest/package_inserts.sql"
psql "$CHAI_DATABASE_URL" -f "$DATA_DIR"/latest/package_inserts.sql

# urls
sed \
  -f "$SED_DIR/urls.sed" "$DATA_DIR/latest/urls.csv" \
  > "$DATA_DIR/latest/url_inserts.sql"
psql "$CHAI_DATABASE_URL" -f "$DATA_DIR"/latest/url_inserts.sql

# versions
# TODO: licenses (license id is annoying)
# TODO: some random parsing errors happening in versions.csv
sed \
  -f "$SED_DIR/versions.sed" "$DATA_DIR/latest/versions.csv" \
  > "$DATA_DIR/latest/version_inserts.sql"
psql "$CHAI_DATABASE_URL" -f "$DATA_DIR"/latest/version_inserts.sql

# package_urls
# TODO: ERROR:  more than one row returned by a subquery used as an expression
sed \
  -f "$SED_DIR/package_url.sed" "$DATA_DIR/latest/package_url.csv" \
  > "$DATA_DIR/latest/package_url_inserts.sql"
psql "$CHAI_DATABASE_URL" -f "$DATA_DIR"/latest/package_url_inserts.sql

# TODO: dependencies -> dependency_type is annoying