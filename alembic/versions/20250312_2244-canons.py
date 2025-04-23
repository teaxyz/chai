"""canons

Revision ID: e7632ae1aff7
Revises: 89af630dc946
Create Date: 2025-03-12 22:44:45.272179

"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "e7632ae1aff7"
down_revision: Union[str, None] = "89af630dc946"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "canons",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("url", sa.String(), nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(), server_default=sa.text("now()"), nullable=False
        ),
        sa.Column(
            "updated_at", sa.DateTime(), server_default=sa.text("now()"), nullable=False
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_canons")),
    )
    op.create_index(op.f("ix_canons_name"), "canons", ["name"], unique=False)
    op.create_index(op.f("ix_canons_url"), "canons", ["url"], unique=True)
    op.create_table(
        "canon_packages",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("canon_id", sa.UUID(), nullable=False),
        sa.Column("package_id", sa.UUID(), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(), server_default=sa.text("now()"), nullable=False
        ),
        sa.Column(
            "updated_at", sa.DateTime(), server_default=sa.text("now()"), nullable=False
        ),
        sa.ForeignKeyConstraint(
            ["canon_id"], ["canons.id"], name=op.f("fk_canon_packages_canon_id_canons")
        ),
        sa.ForeignKeyConstraint(
            ["package_id"],
            ["packages.id"],
            name=op.f("fk_canon_packages_package_id_packages"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_canon_packages")),
    )
    op.create_index(
        op.f("ix_canon_packages_canon_id"), "canon_packages", ["canon_id"], unique=False
    )
    op.create_index(
        op.f("ix_canon_packages_package_id"),
        "canon_packages",
        ["package_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_canon_packages_package_id"), table_name="canon_packages")
    op.drop_index(op.f("ix_canon_packages_canon_id"), table_name="canon_packages")
    op.drop_table("canon_packages")
    op.drop_index(op.f("ix_canons_url"), table_name="canons")
    op.drop_index(op.f("ix_canons_name"), table_name="canons")
    op.drop_table("canons")
