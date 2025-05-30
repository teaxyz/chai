"""rename_canons_table_and_recreate

Revision ID: 542d79f30fc9
Revises: 7392d4d74ce2
Create Date: 2025-05-29 23:41:38.465987

"""

from typing import Sequence, Union

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "542d79f30fc9"
down_revision: Union[str, None] = "7392d4d74ce2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """
    Rename existing canons table and create new one with proper url_id FK
    """
    # Step 1: Rename existing table to preserve data as backup
    op.rename_table("canons", "canons_old")

    # Step 2: Drop FK constraints that pointed to old table
    op.drop_constraint(
        "fk_canon_packages_canon_id_canons", "canon_packages", type_="foreignkey"
    )
    op.drop_constraint("fk_tea_ranks_canon_id_canons", "tea_ranks", type_="foreignkey")

    # Step 3: Create new canons table with proper schema
    op.create_table(
        "canons",
        sa.Column(
            "id",
            UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.func.uuid_generate_v4(),
        ),
        sa.Column(
            "url_id", UUID(as_uuid=True), nullable=False, index=True, unique=True
        ),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()
        ),
        sa.Column(
            "updated_at", sa.DateTime(), nullable=False, server_default=sa.func.now()
        ),
        # Constraints
        sa.ForeignKeyConstraint(["url_id"], ["urls.id"], name="fk_canons_url_id_urls"),
        sa.UniqueConstraint("url_id", name="uq_canons_url_id"),
    )

    # Step 4: Create indexes
    op.create_index("ix_canons_url_id", "canons", ["url_id"])
    op.create_index(
        "ix_canons_name_trgm",
        "canons",
        ["name"],
        postgresql_using="gin",
        postgresql_ops={"name": "gin_trgm_ops"},
    )

    # Note: FK constraints to this table will be recreated in a separate migration
    # after data population, since this table starts empty


def downgrade() -> None:
    """
    Restore original canons table
    """
    # FK constraints were dropped in upgrade and not recreated, so no need to drop them here

    # Drop new table
    op.drop_table("canons")

    # Restore old table
    op.rename_table("canons_old", "canons")

    # Recreate original FK constraints
    op.create_foreign_key(
        "fk_canon_packages_canon_id_canons",
        "canon_packages",
        "canons",
        ["canon_id"],
        ["id"],
    )
    op.create_foreign_key(
        "fk_tea_ranks_canon_id_canons", "tea_ranks", "canons", ["canon_id"], ["id"]
    )
