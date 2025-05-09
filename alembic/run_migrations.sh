#!/bin/bash

set -uo pipefail

# This script sets up the database, runs migrations, and loads initial values

# Wait for database to be ready
until pg_isready -h "$CHAI_DATABASE_URL" -p 5432 -U postgres; do
  echo "waiting for database..."
  sleep 2
done

# Check if the 'chai' database exists, create it if it doesn't
if [ "$( psql -XtAc "SELECT 1 FROM pg_database WHERE datname='chai'" -h "$CHAI_DATABASE_URL" -U postgres)" = '1' ]
then
    echo "Database 'chai' already exists"
else
    echo "Database 'chai' does not exist, creating..."
    # Run the initialization script to create the database
    psql -U postgres -h "$CHAI_DATABASE_URL" -f init-script.sql -a
fi

# Run database migrations
echo "Current database version: $(alembic current)"
if alembic upgrade head
then
  echo "Migrations completed successfully"
else
  echo "Migration failed"
  exit 1
fi

# Load initial values into the database
echo "Loading initial values into the database..."
psql -U postgres -h "$CHAI_DATABASE_URL" -d chai -f load-values.sql -a

echo "Database setup and initialization complete"