FROM ghcr.io/astral-sh/uv:python3.11-bookworm-slim
RUN uv pip install alembic==1.13.2 psycopg2-binary==2.9.10 sqlalchemy==2.0.41 --system
COPY . . 
WORKDIR /alembic
RUN chmod +x /alembic/run_migrations.sh
ENTRYPOINT ["/bin/sh", "/alembic/run_migrations.sh"]
