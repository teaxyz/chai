"""url is not unique

Revision ID: c719192063b5
Revises: 3a8f2c4f018d
Create Date: 2024-10-09 17:13:03.997086

"""

from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "c719192063b5"
down_revision: Union[str, None] = "3a8f2c4f018d"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.drop_index("ix_urls_url", table_name="urls")
    op.create_index(op.f("ix_urls_url"), "urls", ["url"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix_urls_url"), table_name="urls")
    op.create_index("ix_urls_url", "urls", ["url"], unique=True)
