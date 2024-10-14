"""add_users_urls

Revision ID: a97f16d0656a
Revises: 0db06140525f
Create Date: 2024-09-25 08:08:30.574254

"""

# TODO: this one renames the projects table to packages
# it's destructive, so we should definitely redo the migrations
# so that it's clean from here on out

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "a97f16d0656a"
down_revision: Union[str, None] = "0db06140525f"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "package_managers",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("record_created_at", sa.DateTime(), nullable=False),
        sa.Column("record_updated_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_package_managers")),
        sa.UniqueConstraint("name", name=op.f("uq_package_managers_name")),
    )
    op.create_table(
        "url_types",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("record_created_at", sa.DateTime(), nullable=False),
        sa.Column("record_updated_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_url_types")),
        sa.UniqueConstraint("name", name=op.f("uq_url_types_name")),
    )
    op.create_table(
        "urls",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("url", sa.String(), nullable=False),
        sa.Column("record_created_at", sa.DateTime(), nullable=False),
        sa.Column("record_updated_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_urls")),
        sa.UniqueConstraint("url", name=op.f("uq_urls_url")),
    )
    op.create_table(
        "user_types",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("record_created_at", sa.DateTime(), nullable=False),
        sa.Column("record_updated_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_user_types")),
        sa.UniqueConstraint("name", name=op.f("uq_user_types_name")),
    )
    op.create_table(
        "users",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("username", sa.String(), nullable=False),
        sa.Column("record_created_at", sa.DateTime(), nullable=False),
        sa.Column("record_updated_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_users")),
        sa.UniqueConstraint("username", name=op.f("uq_users_username")),
    )
    op.create_table(
        "packages",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("derived_id", sa.String(), nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("package_manager_id", sa.UUID(), nullable=False),
        sa.Column("record_created_at", sa.DateTime(), nullable=False),
        sa.Column("record_updated_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(
            ["package_manager_id"],
            ["package_managers.id"],
            name=op.f("fk_packages_package_manager_id_package_managers"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_packages")),
        sa.UniqueConstraint("derived_id", name=op.f("uq_packages_derived_id")),
    )
    op.create_table(
        "user_urls",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("user_id", sa.UUID(), nullable=False),
        sa.Column("url_id", sa.UUID(), nullable=False),
        sa.Column("user_type_id", sa.UUID(), nullable=False),
        sa.Column("record_created_at", sa.DateTime(), nullable=False),
        sa.Column("record_updated_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(
            ["url_id"], ["urls.id"], name=op.f("fk_user_urls_url_id_urls")
        ),
        sa.ForeignKeyConstraint(
            ["user_id"], ["users.id"], name=op.f("fk_user_urls_user_id_users")
        ),
        sa.ForeignKeyConstraint(
            ["user_type_id"],
            ["user_types.id"],
            name=op.f("fk_user_urls_user_type_id_user_types"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_user_urls")),
    )
    op.create_table(
        "package_urls",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("package_id", sa.UUID(), nullable=False),
        sa.Column("url_id", sa.UUID(), nullable=False),
        sa.Column("url_type_id", sa.UUID(), nullable=False),
        sa.Column("record_created_at", sa.DateTime(), nullable=False),
        sa.Column("record_updated_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(
            ["package_id"],
            ["packages.id"],
            name=op.f("fk_package_urls_package_id_packages"),
        ),
        sa.ForeignKeyConstraint(
            ["url_id"], ["urls.id"], name=op.f("fk_package_urls_url_id_urls")
        ),
        sa.ForeignKeyConstraint(
            ["url_type_id"],
            ["url_types.id"],
            name=op.f("fk_package_urls_url_type_id_url_types"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_package_urls")),
    )
    op.create_table(
        "dependencies",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("version_id", sa.UUID(), nullable=False),
        sa.Column("dependency_id", sa.UUID(), nullable=False),
        sa.Column("dependency_type", sa.String(), nullable=True),
        sa.Column("semver_range", sa.String(), nullable=True),
        sa.Column("record_created_at", sa.DateTime(), nullable=False),
        sa.Column("record_updated_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(
            ["dependency_id"],
            ["packages.id"],
            name=op.f("fk_dependencies_dependency_id_packages"),
        ),
        sa.ForeignKeyConstraint(
            ["version_id"],
            ["versions.id"],
            name=op.f("fk_dependencies_version_id_versions"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_dependencies")),
    )
    op.add_column(
        "load_history", sa.Column("package_manager_id", sa.UUID(), nullable=False)
    )
    op.create_foreign_key(
        op.f("fk_load_history_package_manager_id_package_managers"),
        "load_history",
        "package_managers",
        ["package_manager_id"],
        ["id"],
    )
    op.drop_column("load_history", "package_manager")
    op.add_column("versions", sa.Column("package_id", sa.UUID(), nullable=False))
    op.drop_constraint(
        "fk_versions_project_id_projects", "versions", type_="foreignkey"
    )
    op.create_foreign_key(
        op.f("fk_versions_package_id_packages"),
        "versions",
        "packages",
        ["package_id"],
        ["id"],
    )
    op.drop_column("versions", "project_id")
    # > DANGER ZONE
    # drop the old tables
    op.drop_table("depends_on")
    op.drop_table("projects")
    # < END DANGER ZONE


def downgrade() -> None:
    op.add_column(
        "versions",
        sa.Column("project_id", sa.UUID(), autoincrement=False, nullable=False),
    )
    op.drop_constraint(
        op.f("fk_versions_package_id_packages"), "versions", type_="foreignkey"
    )
    op.create_foreign_key(
        "fk_versions_project_id_projects",
        "versions",
        "projects",
        ["project_id"],
        ["id"],
    )
    op.drop_column("versions", "package_id")
    op.add_column(
        "load_history",
        sa.Column("package_manager", sa.VARCHAR(), autoincrement=False, nullable=False),
    )
    op.drop_constraint(
        op.f("fk_load_history_package_manager_id_package_managers"),
        "load_history",
        type_="foreignkey",
    )
    op.drop_column("load_history", "package_manager_id")
    op.create_table(
        "projects",
        sa.Column("id", sa.UUID(), autoincrement=False, nullable=False),
        sa.Column("derived_id", sa.VARCHAR(), autoincrement=False, nullable=False),
        sa.Column("name", sa.VARCHAR(), autoincrement=False, nullable=False),
        sa.Column("package_manager", sa.VARCHAR(), autoincrement=False, nullable=False),
        sa.Column("repo", sa.VARCHAR(), autoincrement=False, nullable=True),
        sa.Column(
            "record_created_at",
            postgresql.TIMESTAMP(),
            autoincrement=False,
            nullable=False,
        ),
        sa.Column(
            "record_updated_at",
            postgresql.TIMESTAMP(),
            autoincrement=False,
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id", name="pk_projects"),
        sa.UniqueConstraint("derived_id", name="uq_projects_derived_id"),
        postgresql_ignore_search_path=False,
    )
    op.create_table(
        "depends_on",
        sa.Column("id", sa.UUID(), autoincrement=False, nullable=False),
        sa.Column("version_id", sa.UUID(), autoincrement=False, nullable=False),
        sa.Column("dependency_id", sa.UUID(), autoincrement=False, nullable=False),
        sa.Column("dependency_type", sa.VARCHAR(), autoincrement=False, nullable=True),
        sa.Column("semver_range", sa.VARCHAR(), autoincrement=False, nullable=True),
        sa.Column(
            "record_created_at",
            postgresql.TIMESTAMP(),
            autoincrement=False,
            nullable=False,
        ),
        sa.Column(
            "record_updated_at",
            postgresql.TIMESTAMP(),
            autoincrement=False,
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["dependency_id"],
            ["projects.id"],
            name="fk_depends_on_dependency_id_projects",
        ),
        sa.ForeignKeyConstraint(
            ["version_id"], ["versions.id"], name="fk_depends_on_version_id_versions"
        ),
        sa.PrimaryKeyConstraint("id", name="pk_depends_on"),
    )
    op.drop_table("dependencies")
    op.drop_table("package_urls")
    op.drop_table("user_urls")
    op.drop_table("packages")
    op.drop_table("users")
    op.drop_table("user_types")
    op.drop_table("urls")
    op.drop_table("url_types")
    op.drop_table("package_managers")
