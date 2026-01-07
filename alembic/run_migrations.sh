#!/bin/bash

set -uo pipefail

# This script sets up the database, runs migrations, and loads initial values

# NOTE: we don't need to wait for the database to be ready explicitly because docker
# compose already defines the dependency
# Also, in infra, the database would already exist
# until pg_isready -h "$CHAI_DATABASE_URL" -p 5432 -U postgres; do
#   echo "waiting for database..."
#   sleep 2
# done

# Check if the 'chai' database exists, create it if it doesn't
# Parse CHAI_DATABASE_URL: postgresql://user:password@host:port/dbname
# Extract components
DB_USER=$(echo "$CHAI_DATABASE_URL" | sed -n 's|.*://\([^:]*\):.*|\1|p')
DB_PASSWORD=$(echo "$CHAI_DATABASE_URL" | sed -n 's|.*://[^:]*:\([^@]*\)@.*|\1|p')
DB_HOST=$(echo "$CHAI_DATABASE_URL" | sed -n 's|.*@\([^:]*\):.*|\1|p')
DB_PORT=$(echo "$CHAI_DATABASE_URL" | sed -n 's|.*:\([0-9]*\)/.*|\1|p')
DB_NAME=$(echo "$CHAI_DATABASE_URL" | sed -n 's|.*/\([^/]*\)$|\1|p')

export PGPASSWORD="$DB_PASSWORD"

# Connect to 'postgres' database to check if 'chai' exists
if psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname='chai'" | grep -q 1
then
    echo "Database 'chai' already exists"
else
    echo "Database 'chai' does not exist, creating..."
    psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d postgres -f init-script.sql -a
fi

# Run migrations and load data (uses 'chai' database)
echo "Current database version: $(alembic current)"
alembic upgrade head || { echo "Migration failed"; exit 1; }

echo "Loading initial values into the database..."
psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -f load-values.sql -a

echo "Database setup and initialization complete"