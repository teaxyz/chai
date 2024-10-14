"""add license name and id index

Revision ID: d183dcc4bdc8
Revises: c719192063b5
Create Date: 2024-10-10 13:47:01.983756

"""

from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = "d183dcc4bdc8"
down_revision: Union[str, None] = "c719192063b5"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.drop_constraint("uq_licenses_name", "licenses", type_="unique")
    op.create_index(op.f("ix_licenses_name"), "licenses", ["name"], unique=True)
    op.create_index(
        op.f("ix_versions_license_id"), "versions", ["license_id"], unique=False
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_versions_license_id"), table_name="versions")
    op.drop_index(op.f("ix_licenses_name"), table_name="licenses")
    op.create_unique_constraint("uq_licenses_name", "licenses", ["name"])
