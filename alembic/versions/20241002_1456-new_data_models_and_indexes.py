"""new data models and indexes

Revision ID: d1fca65a53c0
Revises: 905522c68f8a
Create Date: 2024-10-02 14:56:06.022022

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "d1fca65a53c0"
down_revision: Union[str, None] = "905522c68f8a"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # add the three new tables
    op.create_table(
        "licenses",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_licenses")),
        sa.UniqueConstraint("name", name=op.f("uq_licenses_name")),
    )
    op.create_table(
        "user_packages",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("user_id", sa.UUID(), nullable=False),
        sa.Column("package_id", sa.UUID(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(
            ["package_id"],
            ["packages.id"],
            name=op.f("fk_user_packages_package_id_packages"),
        ),
        sa.ForeignKeyConstraint(
            ["user_id"], ["users.id"], name=op.f("fk_user_packages_user_id_users")
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_user_packages")),
        sa.UniqueConstraint("user_id", "package_id", name="uq_user_package"),
    )
    op.create_table(
        "user_versions",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("user_id", sa.UUID(), nullable=False),
        sa.Column("version_id", sa.UUID(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(
            ["user_id"], ["users.id"], name=op.f("fk_user_versions_user_id_users")
        ),
        sa.ForeignKeyConstraint(
            ["version_id"],
            ["versions.id"],
            name=op.f("fk_user_versions_version_id_versions"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_user_versions")),
        sa.UniqueConstraint("user_id", "version_id", name="uq_user_version"),
    )

    # rename record_created_at and record_updated_at to created_at and updated_at
    op.add_column(
        "dependencies", sa.Column("created_at", sa.DateTime(), nullable=False)
    )
    op.add_column(
        "dependencies", sa.Column("updated_at", sa.DateTime(), nullable=False)
    )
    op.drop_column("dependencies", "record_created_at")
    op.drop_column("dependencies", "record_updated_at")
    op.add_column(
        "load_history", sa.Column("created_at", sa.DateTime(), nullable=False)
    )
    op.add_column(
        "load_history", sa.Column("updated_at", sa.DateTime(), nullable=False)
    )
    op.drop_column("load_history", "record_created_at")
    op.drop_column("load_history", "record_updated_at")
    op.add_column(
        "package_managers", sa.Column("created_at", sa.DateTime(), nullable=False)
    )
    op.add_column(
        "package_managers", sa.Column("updated_at", sa.DateTime(), nullable=False)
    )
    op.drop_column("package_managers", "record_created_at")
    op.drop_column("package_managers", "record_updated_at")
    op.add_column(
        "package_urls", sa.Column("created_at", sa.DateTime(), nullable=False)
    )
    op.add_column(
        "package_urls", sa.Column("updated_at", sa.DateTime(), nullable=False)
    )
    op.drop_column("package_urls", "record_created_at")
    op.drop_column("package_urls", "record_updated_at")
    op.add_column("packages", sa.Column("import_id", sa.String(), nullable=False))
    op.add_column("packages", sa.Column("readme", sa.String(), nullable=True))
    op.add_column("packages", sa.Column("created_at", sa.DateTime(), nullable=False))
    op.add_column("packages", sa.Column("updated_at", sa.DateTime(), nullable=False))
    op.drop_column("packages", "record_created_at")
    op.drop_column("packages", "record_updated_at")
    op.add_column("url_types", sa.Column("created_at", sa.DateTime(), nullable=False))
    op.add_column("url_types", sa.Column("updated_at", sa.DateTime(), nullable=False))
    op.drop_column("url_types", "record_created_at")
    op.drop_column("url_types", "record_updated_at")
    op.add_column("urls", sa.Column("created_at", sa.DateTime(), nullable=False))
    op.add_column("urls", sa.Column("updated_at", sa.DateTime(), nullable=False))
    op.drop_column("urls", "record_created_at")
    op.drop_column("urls", "record_updated_at")
    op.add_column("user_types", sa.Column("created_at", sa.DateTime(), nullable=False))
    op.add_column("user_types", sa.Column("updated_at", sa.DateTime(), nullable=False))
    op.drop_column("user_types", "record_created_at")
    op.drop_column("user_types", "record_updated_at")
    op.add_column("user_urls", sa.Column("created_at", sa.DateTime(), nullable=False))
    op.add_column("user_urls", sa.Column("updated_at", sa.DateTime(), nullable=False))
    op.drop_column("user_urls", "record_created_at")
    op.drop_column("user_urls", "record_updated_at")
    op.add_column("users", sa.Column("created_at", sa.DateTime(), nullable=False))
    op.add_column("users", sa.Column("updated_at", sa.DateTime(), nullable=False))
    op.drop_column("users", "record_created_at")
    op.drop_column("users", "record_updated_at")
    op.add_column("versions", sa.Column("created_at", sa.DateTime(), nullable=False))
    op.add_column("versions", sa.Column("updated_at", sa.DateTime(), nullable=False))
    op.drop_column("versions", "record_created_at")
    op.drop_column("versions", "record_updated_at")

    # add indexes to import_id and name for packages
    op.create_index(
        op.f("ix_packages_import_id"), "packages", ["import_id"], unique=False
    )
    op.create_index(op.f("ix_packages_name"), "packages", ["name"], unique=False)

    # assert unique constraint on package_manager_id and import_id for packages
    op.create_unique_constraint(
        "uq_package_manager_import_id", "packages", ["package_manager_id", "import_id"]
    )

    # add all the nullable fields to versions
    op.add_column("versions", sa.Column("size", sa.Integer(), nullable=True))
    op.add_column("versions", sa.Column("published_at", sa.DateTime(), nullable=True))
    op.add_column("versions", sa.Column("license_id", sa.UUID(), nullable=True))
    op.add_column("versions", sa.Column("downloads", sa.Integer(), nullable=True))
    op.add_column("versions", sa.Column("checksum", sa.String(), nullable=True))

    # add indexes to downloads, published_at, and size for versions
    op.create_index(
        op.f("ix_versions_downloads"), "versions", ["downloads"], unique=False
    )
    op.create_index(
        op.f("ix_versions_published_at"), "versions", ["published_at"], unique=False
    )
    op.create_index(op.f("ix_versions_size"), "versions", ["size"], unique=False)
    op.create_index(op.f("ix_versions_version"), "versions", ["version"], unique=False)

    # assert unique constraint on package_id and version for versions
    op.create_unique_constraint(
        "uq_package_version", "versions", ["package_id", "version"]
    )
    op.create_foreign_key(
        op.f("fk_versions_license_id_licenses"),
        "versions",
        "licenses",
        ["license_id"],
        ["id"],
    )


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column(
        "versions",
        sa.Column(
            "record_updated_at",
            postgresql.TIMESTAMP(),
            autoincrement=False,
            nullable=False,
        ),
    )
    op.add_column(
        "versions",
        sa.Column(
            "record_created_at",
            postgresql.TIMESTAMP(),
            autoincrement=False,
            nullable=False,
        ),
    )
    op.drop_constraint(
        op.f("fk_versions_license_id_licenses"), "versions", type_="foreignkey"
    )
    op.drop_constraint("uq_package_version", "versions", type_="unique")
    op.drop_index(op.f("ix_versions_version"), table_name="versions")
    op.drop_index(op.f("ix_versions_size"), table_name="versions")
    op.drop_index(op.f("ix_versions_published_at"), table_name="versions")
    op.drop_index(op.f("ix_versions_downloads"), table_name="versions")
    op.drop_column("versions", "updated_at")
    op.drop_column("versions", "created_at")
    op.drop_column("versions", "checksum")
    op.drop_column("versions", "downloads")
    op.drop_column("versions", "license_id")
    op.drop_column("versions", "published_at")
    op.drop_column("versions", "size")
    op.add_column(
        "users",
        sa.Column(
            "record_updated_at",
            postgresql.TIMESTAMP(),
            autoincrement=False,
            nullable=False,
        ),
    )
    op.add_column(
        "users",
        sa.Column(
            "record_created_at",
            postgresql.TIMESTAMP(),
            autoincrement=False,
            nullable=False,
        ),
    )
    op.drop_column("users", "updated_at")
    op.drop_column("users", "created_at")
    op.add_column(
        "user_urls",
        sa.Column(
            "record_updated_at",
            postgresql.TIMESTAMP(),
            autoincrement=False,
            nullable=False,
        ),
    )
    op.add_column(
        "user_urls",
        sa.Column(
            "record_created_at",
            postgresql.TIMESTAMP(),
            autoincrement=False,
            nullable=False,
        ),
    )
    op.drop_column("user_urls", "updated_at")
    op.drop_column("user_urls", "created_at")
    op.add_column(
        "user_types",
        sa.Column(
            "record_updated_at",
            postgresql.TIMESTAMP(),
            autoincrement=False,
            nullable=False,
        ),
    )
    op.add_column(
        "user_types",
        sa.Column(
            "record_created_at",
            postgresql.TIMESTAMP(),
            autoincrement=False,
            nullable=False,
        ),
    )
    op.drop_column("user_types", "updated_at")
    op.drop_column("user_types", "created_at")
    op.add_column(
        "urls",
        sa.Column(
            "record_updated_at",
            postgresql.TIMESTAMP(),
            autoincrement=False,
            nullable=False,
        ),
    )
    op.add_column(
        "urls",
        sa.Column(
            "record_created_at",
            postgresql.TIMESTAMP(),
            autoincrement=False,
            nullable=False,
        ),
    )
    op.drop_column("urls", "updated_at")
    op.drop_column("urls", "created_at")
    op.add_column(
        "url_types",
        sa.Column(
            "record_updated_at",
            postgresql.TIMESTAMP(),
            autoincrement=False,
            nullable=False,
        ),
    )
    op.add_column(
        "url_types",
        sa.Column(
            "record_created_at",
            postgresql.TIMESTAMP(),
            autoincrement=False,
            nullable=False,
        ),
    )
    op.drop_column("url_types", "updated_at")
    op.drop_column("url_types", "created_at")
    op.add_column(
        "packages",
        sa.Column(
            "record_updated_at",
            postgresql.TIMESTAMP(),
            autoincrement=False,
            nullable=False,
        ),
    )
    op.add_column(
        "packages",
        sa.Column(
            "record_created_at",
            postgresql.TIMESTAMP(),
            autoincrement=False,
            nullable=False,
        ),
    )
    op.drop_constraint("uq_package_manager_import_id", "packages", type_="unique")
    op.drop_index(op.f("ix_packages_name"), table_name="packages")
    op.drop_index(op.f("ix_packages_import_id"), table_name="packages")
    op.drop_column("packages", "updated_at")
    op.drop_column("packages", "created_at")
    op.drop_column("packages", "readme")
    op.drop_column("packages", "import_id")
    op.add_column(
        "package_urls",
        sa.Column(
            "record_updated_at",
            postgresql.TIMESTAMP(),
            autoincrement=False,
            nullable=False,
        ),
    )
    op.add_column(
        "package_urls",
        sa.Column(
            "record_created_at",
            postgresql.TIMESTAMP(),
            autoincrement=False,
            nullable=False,
        ),
    )
    op.drop_column("package_urls", "updated_at")
    op.drop_column("package_urls", "created_at")
    op.add_column(
        "package_managers",
        sa.Column(
            "record_updated_at",
            postgresql.TIMESTAMP(),
            autoincrement=False,
            nullable=False,
        ),
    )
    op.add_column(
        "package_managers",
        sa.Column(
            "record_created_at",
            postgresql.TIMESTAMP(),
            autoincrement=False,
            nullable=False,
        ),
    )
    op.drop_column("package_managers", "updated_at")
    op.drop_column("package_managers", "created_at")
    op.add_column(
        "load_history",
        sa.Column(
            "record_updated_at",
            postgresql.TIMESTAMP(),
            autoincrement=False,
            nullable=False,
        ),
    )
    op.add_column(
        "load_history",
        sa.Column(
            "record_created_at",
            postgresql.TIMESTAMP(),
            autoincrement=False,
            nullable=False,
        ),
    )
    op.drop_column("load_history", "updated_at")
    op.drop_column("load_history", "created_at")
    op.add_column(
        "dependencies",
        sa.Column(
            "record_updated_at",
            postgresql.TIMESTAMP(),
            autoincrement=False,
            nullable=False,
        ),
    )
    op.add_column(
        "dependencies",
        sa.Column(
            "record_created_at",
            postgresql.TIMESTAMP(),
            autoincrement=False,
            nullable=False,
        ),
    )
    op.drop_column("dependencies", "updated_at")
    op.drop_column("dependencies", "created_at")
    op.drop_table("user_versions")
    op.drop_table("user_packages")
    op.drop_table("licenses")
    # ### end Alembic commands ###
