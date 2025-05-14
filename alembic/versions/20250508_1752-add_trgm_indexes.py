"""add_trgm_indexes

Revision ID: 7392d4d74ce2
Revises: a41236bd2340
Create Date: 2025-05-08 17:52:40.417822

"""

from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "7392d4d74ce2"
down_revision: Union[str, None] = "a41236bd2340"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Drop the existing indexes
    op.drop_index("ix_canons_name", table_name="canons")
    op.drop_index("ix_urls_url", table_name="urls")

    # Create trigram indexes
    # NOTE: this was added manually to this script (not auto-generated)
    op.create_index(
        "ix_urls_url_trgm",
        "urls",
        ["url"],
        unique=False,
        postgresql_using="gin",
        postgresql_ops={"url": "gin_trgm_ops"},
    )
    op.create_index(
        "ix_canons_name_trgm",
        "canons",
        ["name"],
        unique=False,
        postgresql_using="gin",
        postgresql_ops={"name": "gin_trgm_ops"},
    )


def downgrade() -> None:
    # Drop the trigram indexes
    # NOTE: this was added manually to this script (not auto-generated)
    op.drop_index("ix_urls_url_trgm", table_name="urls")
    op.drop_index("ix_canons_name_trgm", table_name="canons")

    # Recreate the existing indexes (auto-generated)
    op.create_index("ix_urls_url", "urls", ["url"], unique=False)
    op.create_index("ix_canons_name", "canons", ["name"], unique=False)
