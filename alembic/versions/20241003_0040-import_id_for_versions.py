"""import_id for versions

Revision ID: b806c732ebff
Revises: d1fca65a53c0
Create Date: 2024-10-03 00:40:05.834399

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "b806c732ebff"
down_revision: Union[str, None] = "d1fca65a53c0"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("versions", sa.Column("import_id", sa.String(), nullable=False))
    op.create_index(
        op.f("ix_versions_import_id"), "versions", ["import_id"], unique=False
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_versions_import_id"), table_name="versions")
    op.drop_column("versions", "import_id")
