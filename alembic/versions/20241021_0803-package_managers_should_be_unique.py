"""package managers should be unique

Revision ID: 38cc41599874
Revises: 2481138a729a
Create Date: 2024-10-21 08:03:43.647535

"""

from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "38cc41599874"
down_revision: Union[str, None] = "2481138a729a"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_unique_constraint(
        op.f("uq_package_managers_source_id"), "package_managers", ["source_id"]
    )


def downgrade() -> None:
    op.drop_constraint(
        op.f("uq_package_managers_source_id"), "package_managers", type_="unique"
    )
