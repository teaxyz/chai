#!/bin/bash

# Homebrew schedule script
# This script schedules and executes the Homebrew service logging to cron.log

# Set bash options:
# -e: Exit immediately if a command exits with a non-zero status.
# -u: Treat unset variables as an error when substituting.
# -o pipefail: Return value of a pipeline is the status of the last command to exit 
# with a non-zero status.
set -euo pipefail

# Function to log messages with timestamps
log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $1" | tee -a /var/log/cron.log
}

log "Starting scheduler"

# Create the log file
touch /var/log/cron.log

# Set up the cron job
if [ "$TEST" = "true" ]; then
    # In test mode, set the schedule for every two minutes so we can test the scheduling
    echo "*/2 * * * * /usr/bin/env CHAI_DATABASE_URL=$CHAI_DATABASE_URL SOURCE=$SOURCE CODE_DIR=$CODE_DIR DATA_DIR=$DATA_DIR FETCH=$FETCH /package_managers/homebrew/pipeline.sh >> /var/log/cron.log 2>&1" > /etc/cron.d/homebrew-cron
else
    echo "0 */$FREQUENCY * * * /usr/bin/env CHAI_DATABASE_URL=$CHAI_DATABASE_URL SOURCE=$SOURCE CODE_DIR=$CODE_DIR DATA_DIR=$DATA_DIR FETCH=$FETCH /package_managers/homebrew/pipeline.sh >> /var/log/cron.log 2>&1" > /etc/cron.d/homebrew-cron
fi

# Give execution rights on the cron job
chmod 0644 /etc/cron.d/homebrew-cron

# Apply cron job
crontab /etc/cron.d/homebrew-cron

# Run the pipeline script immediately
/package_managers/homebrew/pipeline.sh

# Start cron
log "Starting cron"
cron

# Tail the log file to keep the container running and show logs
tail -f /var/log/cron.log
