"""add now_mean_cms and now_max_cms to reach summaries

Revision ID: 0002_add_now_flow
Revises: 0001_initial
Create Date: 2026-03-20
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "0002_add_now_flow"
down_revision: Union[str, None] = "0001_initial"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "forecast_provider_reach_summaries",
        sa.Column("now_mean_cms", sa.Float(), nullable=True),
    )
    op.add_column(
        "forecast_provider_reach_summaries",
        sa.Column("now_max_cms", sa.Float(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("forecast_provider_reach_summaries", "now_max_cms")
    op.drop_column("forecast_provider_reach_summaries", "now_mean_cms")
