"""redo data model

Revision ID: 8423f70b5354
Revises: b806c732ebff
Create Date: 2024-10-03 15:54:16.969170

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "8423f70b5354"
down_revision: Union[str, None] = "b806c732ebff"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "depends_on_types",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_depends_on_types")),
    )
    op.create_index(
        op.f("ix_depends_on_types_name"), "depends_on_types", ["name"], unique=True
    )
    op.create_table(
        "sources",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("type", sa.String(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_sources")),
        sa.UniqueConstraint("type", name=op.f("uq_sources_type")),
    )
    op.drop_table("user_urls")
    op.drop_table("user_types")
    op.add_column(
        "dependencies", sa.Column("dependency_type_id", sa.UUID(), nullable=True)
    )
    op.drop_constraint("uq_version_dependency_type", "dependencies", type_="unique")
    op.create_unique_constraint(
        "uq_version_dependency_type",
        "dependencies",
        ["version_id", "dependency_id", "dependency_type_id"],
    )
    op.create_index(
        op.f("ix_dependencies_dependency_id"),
        "dependencies",
        ["dependency_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_dependencies_dependency_type_id"),
        "dependencies",
        ["dependency_type_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_dependencies_version_id"), "dependencies", ["version_id"], unique=False
    )
    op.create_foreign_key(
        op.f("fk_dependencies_dependency_type_id_depends_on_types"),
        "dependencies",
        "depends_on_types",
        ["dependency_type_id"],
        ["id"],
    )
    op.drop_column("dependencies", "dependency_type")
    op.add_column("package_managers", sa.Column("source_id", sa.UUID(), nullable=False))
    op.drop_constraint("uq_package_managers_name", "package_managers", type_="unique")
    op.create_foreign_key(
        op.f("fk_package_managers_source_id_sources"),
        "package_managers",
        "sources",
        ["source_id"],
        ["id"],
    )
    op.drop_column("package_managers", "name")
    op.drop_constraint("uq_package_url_type", "package_urls", type_="unique")
    op.create_index(
        op.f("ix_package_urls_package_id"), "package_urls", ["package_id"], unique=False
    )
    op.create_index(
        op.f("ix_package_urls_url_id"), "package_urls", ["url_id"], unique=False
    )
    op.create_unique_constraint(
        "uq_package_url", "package_urls", ["package_id", "url_id"]
    )
    op.drop_constraint(
        "fk_package_urls_url_type_id_url_types", "package_urls", type_="foreignkey"
    )
    op.drop_column("package_urls", "url_type_id")
    op.create_index(
        op.f("ix_packages_package_manager_id"),
        "packages",
        ["package_manager_id"],
        unique=False,
    )
    op.add_column("urls", sa.Column("url_type_id", sa.UUID(), nullable=False))
    op.drop_constraint("uq_urls_url", "urls", type_="unique")
    op.create_index(op.f("ix_urls_url"), "urls", ["url"], unique=True)
    op.create_index(op.f("ix_urls_url_type_id"), "urls", ["url_type_id"], unique=False)
    op.create_unique_constraint("uq_url_type_url", "urls", ["url_type_id", "url"])
    op.create_foreign_key(
        op.f("fk_urls_url_type_id_url_types"),
        "urls",
        "url_types",
        ["url_type_id"],
        ["id"],
    )
    op.create_index(
        op.f("ix_user_packages_package_id"),
        "user_packages",
        ["package_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_user_packages_user_id"), "user_packages", ["user_id"], unique=False
    )
    op.create_index(
        op.f("ix_user_versions_user_id"), "user_versions", ["user_id"], unique=False
    )
    op.create_index(
        op.f("ix_user_versions_version_id"),
        "user_versions",
        ["version_id"],
        unique=False,
    )
    op.add_column("users", sa.Column("source_id", sa.UUID(), nullable=False))
    op.add_column("users", sa.Column("import_id", sa.String(), nullable=False))
    op.drop_constraint("uq_users_username", "users", type_="unique")
    op.create_index(op.f("ix_users_import_id"), "users", ["import_id"], unique=True)
    op.create_index(op.f("ix_users_source_id"), "users", ["source_id"], unique=False)
    op.create_index(op.f("ix_users_username"), "users", ["username"], unique=True)
    op.create_unique_constraint(
        "uq_source_import_id", "users", ["source_id", "import_id"]
    )
    op.create_foreign_key(
        op.f("fk_users_source_id_sources"), "users", "sources", ["source_id"], ["id"]
    )
    op.create_index(
        op.f("ix_versions_package_id"), "versions", ["package_id"], unique=False
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_versions_package_id"), table_name="versions")
    op.drop_constraint(op.f("fk_users_source_id_sources"), "users", type_="foreignkey")
    op.drop_constraint("uq_source_import_id", "users", type_="unique")
    op.drop_index(op.f("ix_users_username"), table_name="users")
    op.drop_index(op.f("ix_users_source_id"), table_name="users")
    op.drop_index(op.f("ix_users_import_id"), table_name="users")
    op.create_unique_constraint("uq_users_username", "users", ["username"])
    op.drop_column("users", "import_id")
    op.drop_column("users", "source_id")
    op.drop_index(op.f("ix_user_versions_version_id"), table_name="user_versions")
    op.drop_index(op.f("ix_user_versions_user_id"), table_name="user_versions")
    op.drop_index(op.f("ix_user_packages_user_id"), table_name="user_packages")
    op.drop_index(op.f("ix_user_packages_package_id"), table_name="user_packages")
    op.drop_constraint(
        op.f("fk_urls_url_type_id_url_types"), "urls", type_="foreignkey"
    )
    op.drop_constraint("uq_url_type_url", "urls", type_="unique")
    op.drop_index(op.f("ix_urls_url_type_id"), table_name="urls")
    op.drop_index(op.f("ix_urls_url"), table_name="urls")
    op.create_unique_constraint("uq_urls_url", "urls", ["url"])
    op.drop_column("urls", "url_type_id")
    op.drop_index(op.f("ix_packages_package_manager_id"), table_name="packages")
    op.add_column(
        "package_urls",
        sa.Column("url_type_id", sa.UUID(), autoincrement=False, nullable=False),
    )
    op.create_foreign_key(
        "fk_package_urls_url_type_id_url_types",
        "package_urls",
        "url_types",
        ["url_type_id"],
        ["id"],
    )
    op.drop_constraint("uq_package_url", "package_urls", type_="unique")
    op.drop_index(op.f("ix_package_urls_url_id"), table_name="package_urls")
    op.drop_index(op.f("ix_package_urls_package_id"), table_name="package_urls")
    op.create_unique_constraint(
        "uq_package_url_type", "package_urls", ["package_id", "url_id", "url_type_id"]
    )
    op.add_column(
        "package_managers",
        sa.Column("name", sa.VARCHAR(), autoincrement=False, nullable=False),
    )
    op.drop_constraint(
        op.f("fk_package_managers_source_id_sources"),
        "package_managers",
        type_="foreignkey",
    )
    op.create_unique_constraint(
        "uq_package_managers_name", "package_managers", ["name"]
    )
    op.drop_column("package_managers", "source_id")
    op.add_column(
        "dependencies",
        sa.Column("dependency_type", sa.VARCHAR(), autoincrement=False, nullable=True),
    )
    op.drop_constraint(
        op.f("fk_dependencies_dependency_type_id_depends_on_types"),
        "dependencies",
        type_="foreignkey",
    )
    op.drop_index(op.f("ix_dependencies_version_id"), table_name="dependencies")
    op.drop_index(op.f("ix_dependencies_dependency_type_id"), table_name="dependencies")
    op.drop_index(op.f("ix_dependencies_dependency_id"), table_name="dependencies")
    op.drop_constraint("uq_version_dependency_type", "dependencies", type_="unique")
    op.create_unique_constraint(
        "uq_version_dependency_type",
        "dependencies",
        ["version_id", "dependency_id", "dependency_type"],
    )
    op.drop_column("dependencies", "dependency_type_id")
    op.create_table(
        "user_types",
        sa.Column("id", sa.UUID(), autoincrement=False, nullable=False),
        sa.Column("name", sa.VARCHAR(), autoincrement=False, nullable=False),
        sa.Column(
            "created_at", postgresql.TIMESTAMP(), autoincrement=False, nullable=False
        ),
        sa.Column(
            "updated_at", postgresql.TIMESTAMP(), autoincrement=False, nullable=False
        ),
        sa.PrimaryKeyConstraint("id", name="pk_user_types"),
        sa.UniqueConstraint("name", name="uq_user_types_name"),
        postgresql_ignore_search_path=False,
    )
    op.create_table(
        "user_urls",
        sa.Column("id", sa.UUID(), autoincrement=False, nullable=False),
        sa.Column("user_id", sa.UUID(), autoincrement=False, nullable=False),
        sa.Column("url_id", sa.UUID(), autoincrement=False, nullable=False),
        sa.Column("user_type_id", sa.UUID(), autoincrement=False, nullable=False),
        sa.Column(
            "created_at", postgresql.TIMESTAMP(), autoincrement=False, nullable=False
        ),
        sa.Column(
            "updated_at", postgresql.TIMESTAMP(), autoincrement=False, nullable=False
        ),
        sa.ForeignKeyConstraint(
            ["url_id"], ["urls.id"], name="fk_user_urls_url_id_urls"
        ),
        sa.ForeignKeyConstraint(
            ["user_id"], ["users.id"], name="fk_user_urls_user_id_users"
        ),
        sa.ForeignKeyConstraint(
            ["user_type_id"],
            ["user_types.id"],
            name="fk_user_urls_user_type_id_user_types",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_user_urls"),
        sa.UniqueConstraint(
            "user_id", "url_id", "user_type_id", name="uq_user_url_type"
        ),
    )
    op.drop_table("sources")
    op.drop_index(op.f("ix_depends_on_types_name"), table_name="depends_on_types")
    op.drop_table("depends_on_types")
