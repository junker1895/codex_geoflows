"""add composite index on (provider, run_id, severity_score)

Revision ID: 0003_severity_idx
Revises: 0002_add_now_flow
Create Date: 2026-03-22
"""

from typing import Sequence, Union

from alembic import op


revision: str = "0003_severity_idx"
down_revision: Union[str, None] = "0002_add_now_flow"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_index(
        "ix_summary_provider_run_severity",
        "forecast_provider_reach_summaries",
        ["provider", "run_id", "severity_score"],
    )


def downgrade() -> None:
    op.drop_index("ix_summary_provider_run_severity", table_name="forecast_provider_reach_summaries")
