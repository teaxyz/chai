#!/bin/bash

# Homebrew Pipeline Script
# This script fetches, transforms, and loads Homebrew package data into a PostgreSQL database.

# Set bash options:
# -e: Exit immediately if a command exits with a non-zero status.
# -x: Print commands and their arguments as they are executed.
# -u: Treat unset variables as an error when substituting.
# -o pipefail: Return value of a pipeline is the status of the last command to exit with a non-zero status.
set -uo pipefail

# Function to log messages with timestamps
log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $1" | tee -a /var/log/cron.log
}

log "Starting Homebrew pipeline script"

# Fetch required IDs and URLs from the database
log "Fetching required IDs and URLs from the database"
IDS=$(psql "$CHAI_DATABASE_URL" -f /package_managers/homebrew/sql/homebrew_vars.sql -t -A -F'|')

# Parse the results
IFS='|' read -r \
    PACKAGE_MANAGER_ID \
    HOMEPAGE_URL_TYPE_ID \
    SOURCE_URL_TYPE_ID \
    BUILD_DEPENDS_ON_TYPE_ID \
    RUNTIME_DEPENDS_ON_TYPE_ID \
    RECOMMENDED_DEPENDS_ON_TYPE_ID \
    OPTIONAL_DEPENDS_ON_TYPE_ID \
    TEST_DEPENDS_ON_TYPE_ID <<< "$IDS"

# Validate that all required IDs are present and export them
required_vars=(
    PACKAGE_MANAGER_ID
    HOMEPAGE_URL_TYPE_ID
    SOURCE_URL_TYPE_ID
    BUILD_DEPENDS_ON_TYPE_ID
    RUNTIME_DEPENDS_ON_TYPE_ID
    RECOMMENDED_DEPENDS_ON_TYPE_ID
    OPTIONAL_DEPENDS_ON_TYPE_ID
    TEST_DEPENDS_ON_TYPE_ID
)

for var in "${required_vars[@]}"; do
    if [ -z "${!var}" ]; then
        log "ERROR: Required variable $var is empty or unset. Exiting."
        exit 1
    fi
    # shellcheck disable=SC2163
    export "$var"
done

# Data fetching and processing
if [ "$FETCH" = true ]; then
    log "Fetching new data from Homebrew"

    # Create timestamped directory for this run
    NOW=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
    mkdir -p "$DATA_DIR"/"$NOW"

    # Download source data
    log "Downloading source data"
    curl -s "$SOURCE" > "$DATA_DIR"/"$NOW"/source.json

    # Update 'latest' symlink
    ln -sfn "$NOW" "$DATA_DIR"/latest

    # Transform data using jq scripts
    log "Transforming data"
    for x in "$CODE_DIR"/jq/*.jq; do
        filename=$(basename "$x" .jq)
        log "Processing $filename"
        case "$filename" in
            packages)
                jq -f "$x" -r \
                    --arg package_manager_id "$PACKAGE_MANAGER_ID" \
                    "$DATA_DIR"/latest/source.json > "$DATA_DIR"/latest/"${filename}".sql
                ;;
            urls)
                jq -f "$x" -r \
                    --arg homepage_url_type_id "$HOMEPAGE_URL_TYPE_ID" \
                    --arg source_url_type_id "$SOURCE_URL_TYPE_ID" \
                    "$DATA_DIR"/latest/source.json > "$DATA_DIR"/latest/"${filename}".sql
                ;;
            versions)
                jq -f "$x" -r \
                    "$DATA_DIR"/latest/source.json > "$DATA_DIR"/latest/"${filename}".sql
                ;;
            package_url)
                jq -f "$x" -r \
                    --arg homepage_url_type_id "$HOMEPAGE_URL_TYPE_ID" \
                    --arg source_url_type_id "$SOURCE_URL_TYPE_ID" \
                    "$DATA_DIR"/latest/source.json > "$DATA_DIR"/latest/"${filename}".sql
                ;;
            dependencies)
                jq -f "$x" -r \
                    --arg build_deps_type_id "$BUILD_DEPENDS_ON_TYPE_ID" \
                    --arg runtime_deps_type_id "$RUNTIME_DEPENDS_ON_TYPE_ID" \
                    --arg recommended_deps_type_id "$RECOMMENDED_DEPENDS_ON_TYPE_ID" \
                    --arg optional_deps_type_id "$OPTIONAL_DEPENDS_ON_TYPE_ID" \
                    --arg test_deps_type_id "$TEST_DEPENDS_ON_TYPE_ID" \
                    "$DATA_DIR"/latest/source.json > "$DATA_DIR"/latest/"${filename}".sql
                ;;
            *)
                log "Skipping unknown file: $filename"
                ;;
        esac
    done
else
    log "Skipping data fetch (FETCH=false)"
fi

# Load data into database
log "Loading data into database"
psql -q "$CHAI_DATABASE_URL" <<EOSQL
\i $DATA_DIR/latest/packages.sql
\i $DATA_DIR/latest/urls.sql
\i $DATA_DIR/latest/versions.sql
\i $DATA_DIR/latest/package_url.sql
\i $DATA_DIR/latest/dependencies.sql
EOSQL

log "Homebrew pipeline completed successfully"
