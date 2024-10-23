#!/bin/bash

set -uo pipefail

# Create the log fil
touch /var/log/cron.log

# Set up the cron job
if [ "$TEST" = "true" ]; then
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
cron

# Tail the log file to keep the container running and show logs
tail -f /var/log/cron.log