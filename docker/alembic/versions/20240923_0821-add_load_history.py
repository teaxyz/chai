"""add_load_history

Revision ID: 0db06140525f
Revises: 14ea939f4bf7
Create Date: 2024-09-23 08:21:52.454272

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0db06140525f"
down_revision: Union[str, None] = "14ea939f4bf7"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "load_history",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("package_manager", sa.String(), nullable=False),
        sa.Column("record_created_at", sa.DateTime(), nullable=False),
        sa.Column("record_updated_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_load_history")),
    )


def downgrade() -> None:
    op.drop_table("load_history")
