FROM ubuntu:24.10
# RUN pkgx install alembic.sqlalchemy.org^1 psycopg.org/psycopg2^2 postgresql.org^16
RUN apt -y update && apt -y upgrade
RUN apt -y install postgresql
RUN apt -y install alembic
RUN apt -y install python3-psycopg2
RUN apt -y install python3-sqlalchemy python3-sqlalchemy-ext
COPY . .
WORKDIR /alembic
RUN chmod +x run_migrations.sh
ENTRYPOINT ["/alembic/run_migrations.sh"]
