#!/bin/bash

# wait for db to be ready
until pg_isready -h db -p 5432 -U postgres; do
  echo "waiting for database..."
  sleep 2
done

# create db if needed
# if [ "$( psql -XtAc "SELECT 1 FROM pg_database WHERE datname='chai'" 2&>/dev/null)" = '1' ]
if [ "$( psql -XtAc "SELECT 1 FROM pg_database WHERE datname='chai'" -h db -U postgres)" = '1' ]
then
    echo "Database 'chai' already exists"
else
    echo "Database 'chai' does not exist, creating..."
    psql -U postgres -h db -f init-script.sql -a
fi

# migrate
echo "db currently at $(alembic current)"
if alembic upgrade head
then
  echo "migrations run successfully"
else
  echo "migrations failed"
  exit 1
fi
