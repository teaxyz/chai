"""add-legacy-dependency-table

Revision ID: 89af630dc946
Revises: 238d591d5310
Create Date: 2025-03-12 00:45:35.727521

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "89af630dc946"
down_revision: str | None = "238d591d5310"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "legacy_dependencies",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("package_id", sa.UUID(), nullable=False),
        sa.Column("dependency_id", sa.UUID(), nullable=False),
        sa.Column("dependency_type_id", sa.UUID(), nullable=False),
        sa.Column("semver_range", sa.String(), nullable=True),
        sa.Column(
            "created_at", sa.DateTime(), server_default=sa.text("now()"), nullable=False
        ),
        sa.Column(
            "updated_at", sa.DateTime(), server_default=sa.text("now()"), nullable=False
        ),
        sa.ForeignKeyConstraint(
            ["dependency_id"],
            ["packages.id"],
            name=op.f("fk_legacy_dependencies_dependency_id_packages"),
        ),
        sa.ForeignKeyConstraint(
            ["dependency_type_id"],
            ["depends_on_types.id"],
            name=op.f("fk_legacy_dependencies_dependency_type_id_depends_on_types"),
        ),
        sa.ForeignKeyConstraint(
            ["package_id"],
            ["packages.id"],
            name=op.f("fk_legacy_dependencies_package_id_packages"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_legacy_dependencies")),
        sa.UniqueConstraint(
            "package_id", "dependency_id", name="uq_package_dependency"
        ),
    )
    op.create_index(
        op.f("ix_legacy_dependencies_dependency_id"),
        "legacy_dependencies",
        ["dependency_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_legacy_dependencies_dependency_type_id"),
        "legacy_dependencies",
        ["dependency_type_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_legacy_dependencies_package_id"),
        "legacy_dependencies",
        ["package_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        op.f("ix_legacy_dependencies_package_id"), table_name="legacy_dependencies"
    )
    op.drop_index(
        op.f("ix_legacy_dependencies_dependency_type_id"),
        table_name="legacy_dependencies",
    )
    op.drop_index(
        op.f("ix_legacy_dependencies_dependency_id"), table_name="legacy_dependencies"
    )
    op.drop_table("legacy_dependencies")
