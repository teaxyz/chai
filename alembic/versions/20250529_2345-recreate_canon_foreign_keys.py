"""recreate_canon_foreign_keys

Revision ID: 3de32bb99a71
Revises: 542d79f30fc9
Create Date: 2025-05-29 23:45:12.372951

"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "3de32bb99a71"
down_revision: str | None = "542d79f30fc9"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """
    Recreate FK constraints pointing to canons table after data population
    Run this AFTER your canonicalization script has populated the canons table
    """
    # First, clean up any orphaned records in referencing tables
    # (Optional: uncomment if you want to auto-clean orphaned data)
    # op.execute("""
    #     DELETE FROM canon_packages
    #     WHERE canon_id NOT IN (SELECT id FROM canons)
    # """)
    # op.execute("""
    #     DELETE FROM tea_ranks
    #     WHERE canon_id NOT IN (SELECT id FROM canons)
    # """)

    # Recreate FK constraints
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


def downgrade() -> None:
    """
    Drop FK constraints pointing to canons table
    """
    op.drop_constraint(
        "fk_canon_packages_canon_id_canons", "canon_packages", type_="foreignkey"
    )
    op.drop_constraint("fk_tea_ranks_canon_id_canons", "tea_ranks", type_="foreignkey")
