"""add-unique-package-to-canon-packages

Revision ID: a41236bd2340
Revises: 26e124131bf8
Create Date: 2025-04-22 09:40:22.901637

"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "a41236bd2340"
down_revision: str | None = "26e124131bf8"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.drop_index("ix_canon_packages_package_id", table_name="canon_packages")
    op.create_index(
        op.f("ix_canon_packages_package_id"),
        "canon_packages",
        ["package_id"],
        unique=True,
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_canon_packages_package_id"), table_name="canon_packages")
    op.create_index(
        "ix_canon_packages_package_id", "canon_packages", ["package_id"], unique=False
    )
