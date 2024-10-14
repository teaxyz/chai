"""link tables add unique constraints

Revision ID: 905522c68f8a
Revises: a97f16d0656a
Create Date: 2024-09-30 20:34:54.717496

"""

from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = "905522c68f8a"
down_revision: Union[str, None] = "a97f16d0656a"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_unique_constraint(
        "uq_version_dependency_type",
        "dependencies",
        ["version_id", "dependency_id", "dependency_type"],
    )
    op.create_unique_constraint(
        "uq_package_url_type", "package_urls", ["package_id", "url_id", "url_type_id"]
    )
    op.create_unique_constraint(
        "uq_user_url_type", "user_urls", ["user_id", "url_id", "user_type_id"]
    )


def downgrade() -> None:
    op.drop_constraint("uq_user_url_type", "user_urls", type_="unique")
    op.drop_constraint("uq_package_url_type", "package_urls", type_="unique")
    op.drop_constraint("uq_version_dependency_type", "dependencies", type_="unique")
