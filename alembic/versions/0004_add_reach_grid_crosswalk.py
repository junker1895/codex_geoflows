"""add reach_grid_crosswalk table for grid-based provider mapping

Revision ID: 0004_crosswalk
Revises: 0003_severity_idx
Create Date: 2026-03-22
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0004_crosswalk"
down_revision: Union[str, None] = "0003_severity_idx"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "reach_grid_crosswalk",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("reach_id", sa.String(128), nullable=False, index=True),
        sa.Column("target_provider", sa.String(64), nullable=False),
        sa.Column("grid_lat", sa.Float, nullable=False),
        sa.Column("grid_lon", sa.Float, nullable=False),
        sa.Column("upstream_area_km2", sa.Float, nullable=True),
        sa.Column("distance_km", sa.Float, nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.UniqueConstraint("reach_id", "target_provider", name="uq_crosswalk_reach_provider"),
    )
    op.create_index("ix_crosswalk_provider", "reach_grid_crosswalk", ["target_provider"])


def downgrade() -> None:
    op.drop_index("ix_crosswalk_provider", table_name="reach_grid_crosswalk")
    op.drop_table("reach_grid_crosswalk")
