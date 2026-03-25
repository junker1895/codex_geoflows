"""expand reach_grid_crosswalk for hydrologic diagnostics

Revision ID: 0005_crosswalk_diag
Revises: 0004_crosswalk
Create Date: 2026-03-25
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0005_crosswalk_diag"
down_revision: Union[str, None] = "0004_crosswalk"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.alter_column("reach_grid_crosswalk", "grid_lat", existing_type=sa.Float(), nullable=True)
    op.alter_column("reach_grid_crosswalk", "grid_lon", existing_type=sa.Float(), nullable=True)

    op.add_column("reach_grid_crosswalk", sa.Column("reach_upstream_area_km2", sa.Float(), nullable=True))
    op.add_column("reach_grid_crosswalk", sa.Column("grid_upstream_area_km2", sa.Float(), nullable=True))
    op.add_column("reach_grid_crosswalk", sa.Column("area_ratio", sa.Float(), nullable=True))
    op.add_column("reach_grid_crosswalk", sa.Column("match_score", sa.Float(), nullable=True))
    op.add_column("reach_grid_crosswalk", sa.Column("match_method", sa.String(length=64), nullable=True))
    op.add_column(
        "reach_grid_crosswalk",
        sa.Column("is_valid_match", sa.Boolean(), nullable=False, server_default=sa.text("true")),
    )

    op.execute(
        """
        UPDATE reach_grid_crosswalk
        SET reach_upstream_area_km2 = upstream_area_km2
        WHERE reach_upstream_area_km2 IS NULL
        """
    )


def downgrade() -> None:
    op.drop_column("reach_grid_crosswalk", "is_valid_match")
    op.drop_column("reach_grid_crosswalk", "match_method")
    op.drop_column("reach_grid_crosswalk", "match_score")
    op.drop_column("reach_grid_crosswalk", "area_ratio")
    op.drop_column("reach_grid_crosswalk", "grid_upstream_area_km2")
    op.drop_column("reach_grid_crosswalk", "reach_upstream_area_km2")
    op.alter_column("reach_grid_crosswalk", "grid_lon", existing_type=sa.Float(), nullable=False)
    op.alter_column("reach_grid_crosswalk", "grid_lat", existing_type=sa.Float(), nullable=False)
