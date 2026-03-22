"""add covering index on (provider, run_id, severity_score) INCLUDE (provider_reach_id)

Revision ID: 0003_severity_idx
Revises: 0002_add_now_flow
Create Date: 2026-03-22
"""

from typing import Sequence, Union

from alembic import op
from sqlalchemy import text


revision: str = "0003_severity_idx"
down_revision: Union[str, None] = "0002_add_now_flow"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Drop the old non-covering index if it exists
    op.execute(text("DROP INDEX IF EXISTS ix_summary_provider_run_severity"))
    # Covering index: severity_score DESC so LIMIT grabs worst-first without a sort.
    # INCLUDE provider_reach_id so the query never touches the heap.
    op.execute(text(
        "CREATE INDEX ix_summary_provider_run_severity "
        "ON forecast_provider_reach_summaries (provider, run_id, severity_score DESC) "
        "INCLUDE (provider_reach_id)"
    ))


def downgrade() -> None:
    op.drop_index("ix_summary_provider_run_severity", table_name="forecast_provider_reach_summaries")
