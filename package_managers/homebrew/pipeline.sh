#!/bin/bash

set -exuo pipefail

# get all the required IDs and URLs from the database
IDS=$(psql "$CHAI_DATABASE_URL" -f sql/homebrew_vars.sql -t -A -F'|')

# Parse the results and export variables
IFS='|' read -r PACKAGE_MANAGER_ID HOMEPAGE_URL_TYPE_ID SOURCE_URL_TYPE_ID \
    BUILD_DEPENDS_ON_TYPE_ID RUNTIME_DEPENDS_ON_TYPE_ID \
    RECOMMENDED_DEPENDS_ON_TYPE_ID OPTIONAL_DEPENDS_ON_TYPE_ID \
    TEST_DEPENDS_ON_TYPE_ID USES_FROM_MACOS_DEPENDS_ON_TYPE_ID <<< "$IDS"

export PACKAGE_MANAGER_ID
export HOMEPAGE_URL_TYPE_ID
export SOURCE_URL_TYPE_ID
export BUILD_DEPENDS_ON_TYPE_ID
export RUNTIME_DEPENDS_ON_TYPE_ID
export RECOMMENDED_DEPENDS_ON_TYPE_ID
export OPTIONAL_DEPENDS_ON_TYPE_ID
export TEST_DEPENDS_ON_TYPE_ID
export USES_FROM_MACOS_DEPENDS_ON_TYPE_ID

# if any of the IDs are empty, exit
if [ -z "$PACKAGE_MANAGER_ID" ] || [ -z "$HOMEPAGE_URL_TYPE_ID" ] || [ -z "$SOURCE_URL_TYPE_ID" ] || [ -z "$BUILD_DEPENDS_ON_TYPE_ID" ] || [ -z "$RUNTIME_DEPENDS_ON_TYPE_ID" ] || [ -z "$RECOMMENDED_DEPENDS_ON_TYPE_ID" ] || [ -z "$OPTIONAL_DEPENDS_ON_TYPE_ID" ] || [ -z "$TEST_DEPENDS_ON_TYPE_ID" ] || [ -z "$USES_FROM_MACOS_DEPENDS_ON_TYPE_ID" ]; then
    echo "One or more IDs are empty. Exiting."
    exit 1
fi

# if you've already pulled the Homebrew data, you can `export FETCH=false` to skip the
# download, and just work off the latest symlink

# > [!IMPORTANT]
# >
# > ONLY WORKS IF THE VOLUMES ARE MOUNTED

if [ "$FETCH" = true ]; then
  NOW=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
  mkdir -p "$DATA_DIR"/"$NOW"

  # extract
  curl -s "$SOURCE" > "$DATA_DIR"/"$NOW"/source.json

  # make a symlink called latest, pointing to $NOW
  ln -sfn "$NOW" "$DATA_DIR"/latest

  # transform
  for x in "$CODE_DIR"/jq/*.jq; do
    filename=$(basename "$x" .jq)
    # use the formulas defined in the jq folder for each data model
    if [ "$filename" = "packages" ]; then
      jq -f "$x" -r --arg package_manager_id "$PACKAGE_MANAGER_ID" "$DATA_DIR"/latest/source.json > "$DATA_DIR"/latest/"${filename}".sql
    elif [ "$filename" = "urls" ]; then
      jq -f "$x" -r \
        --arg homepage_url_type_id "$HOMEPAGE_URL_TYPE_ID" \
        --arg source_url_type_id "$SOURCE_URL_TYPE_ID" \
        "$DATA_DIR"/latest/source.json > "$DATA_DIR"/latest/"${filename}".sql
    elif [ "$filename" = "versions" ]; then
      jq -f "$x" -r \
        "$DATA_DIR"/latest/source.json > "$DATA_DIR"/latest/"${filename}".sql
    elif [ "$filename" = "package_url" ]; then
      jq -f "$x" -r \
        --arg homepage_url_type_id "$HOMEPAGE_URL_TYPE_ID" \
        --arg source_url_type_id "$SOURCE_URL_TYPE_ID" \
        "$DATA_DIR"/latest/source.json > "$DATA_DIR"/latest/"${filename}".sql
    elif [ "$filename" = "dependencies" ]; then
      jq -f "$x" -r \
        --arg build_deps_type_id "$BUILD_DEPENDS_ON_TYPE_ID" \
        --arg runtime_deps_type_id "$RUNTIME_DEPENDS_ON_TYPE_ID" \
        --arg recommended_deps_type_id "$RECOMMENDED_DEPENDS_ON_TYPE_ID" \
        --arg optional_deps_type_id "$OPTIONAL_DEPENDS_ON_TYPE_ID" \
        --arg test_deps_type_id "$TEST_DEPENDS_ON_TYPE_ID" \
        --arg uses_from_macos_type_id "$USES_FROM_MACOS_DEPENDS_ON_TYPE_ID" \
        "$DATA_DIR"/latest/source.json > "$DATA_DIR"/latest/"${filename}".sql
    else
      echo "skipping $filename"
    fi
  done
fi

# load - order matters
psql "$CHAI_DATABASE_URL" -f "$DATA_DIR"/latest/packages.sql
psql "$CHAI_DATABASE_URL" -f "$DATA_DIR"/latest/urls.sql
psql "$CHAI_DATABASE_URL" -f "$DATA_DIR"/latest/versions.sql
psql "$CHAI_DATABASE_URL" -f "$DATA_DIR"/latest/package_url.sql
psql "$CHAI_DATABASE_URL" -f "$DATA_DIR"/latest/dependencies.sql
