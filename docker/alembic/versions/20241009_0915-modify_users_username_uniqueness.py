"""modify users username uniqueness

Revision ID: 3a8f2c4f018d
Revises: 8423f70b5354
Create Date: 2024-10-09 09:15:20.652572

"""

from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = "3a8f2c4f018d"
down_revision: Union[str, None] = "8423f70b5354"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.drop_constraint("uq_source_import_id", "users", type_="unique")
    op.drop_index("ix_users_import_id", table_name="users")
    op.create_index(op.f("ix_users_import_id"), "users", ["import_id"], unique=False)
    op.drop_index("ix_users_username", table_name="users")
    op.create_index(op.f("ix_users_username"), "users", ["username"], unique=False)
    op.create_unique_constraint(
        "uq_source_username", "users", ["source_id", "username"]
    )


def downgrade() -> None:
    op.drop_constraint("uq_source_username", "users", type_="unique")
    op.drop_index(op.f("ix_users_username"), table_name="users")
    op.create_index("ix_users_username", "users", ["username"], unique=True)
    op.drop_index(op.f("ix_users_import_id"), table_name="users")
    op.create_index("ix_users_import_id", "users", ["import_id"], unique=True)
    op.create_unique_constraint(
        "uq_source_import_id", "users", ["source_id", "import_id"]
    )
