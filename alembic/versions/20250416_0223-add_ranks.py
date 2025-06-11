"""add-ranks

Revision ID: 26e124131bf8
Revises: e7632ae1aff7
Create Date: 2025-04-16 02:23:33.665773

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "26e124131bf8"
down_revision: str | None = "e7632ae1aff7"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "tea_rank_runs",
        sa.Column(
            "id",
            sa.UUID(),
            server_default=sa.text("uuid_generate_v4()"),
            nullable=False,
        ),
        sa.Column("run", sa.Integer(), nullable=False),
        sa.Column("split_ratio", sa.String(), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(), server_default=sa.text("now()"), nullable=False
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_tea_rank_runs")),
    )
    op.create_table(
        "tea_ranks",
        sa.Column(
            "id",
            sa.UUID(),
            server_default=sa.text("uuid_generate_v4()"),
            nullable=False,
        ),
        sa.Column("tea_rank_run", sa.Integer(), nullable=False),
        sa.Column("canon_id", sa.UUID(), nullable=False),
        sa.Column("rank", sa.String(), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(), server_default=sa.text("now()"), nullable=False
        ),
        sa.ForeignKeyConstraint(
            ["canon_id"], ["canons.id"], name=op.f("fk_tea_ranks_canon_id_canons")
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_tea_ranks")),
    )
    op.create_index(
        op.f("ix_tea_ranks_canon_id"), "tea_ranks", ["canon_id"], unique=False
    )
    op.create_index(
        op.f("ix_tea_ranks_tea_rank_run"), "tea_ranks", ["tea_rank_run"], unique=False
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_tea_ranks_tea_rank_run"), table_name="tea_ranks")
    op.drop_index(op.f("ix_tea_ranks_canon_id"), table_name="tea_ranks")
    op.drop_table("tea_ranks")
    op.drop_table("tea_rank_runs")
