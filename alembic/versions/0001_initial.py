"""initial schema

Revision ID: 0001_initial
Revises: 
Create Date: 2026-03-11
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "0001_initial"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "forecast_runs",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("provider", sa.String(length=64), nullable=False),
        sa.Column("run_id", sa.String(length=128), nullable=False),
        sa.Column("run_date_utc", sa.DateTime(timezone=True), nullable=False),
        sa.Column("issued_at_utc", sa.DateTime(timezone=True), nullable=True),
        sa.Column("source_type", sa.String(length=64), nullable=False),
        sa.Column("ingest_status", sa.String(length=32), nullable=False),
        sa.Column("metadata_json", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint("provider", "run_id", name="uq_forecast_run_provider_run_id"),
    )
    op.create_index(op.f("ix_forecast_runs_provider"), "forecast_runs", ["provider"])
    op.create_index(op.f("ix_forecast_runs_run_id"), "forecast_runs", ["run_id"])

    op.create_table(
        "forecast_provider_return_periods",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("provider", sa.String(length=64), nullable=False),
        sa.Column("provider_reach_id", sa.String(length=128), nullable=False),
        sa.Column("rp_2", sa.Float(), nullable=True),
        sa.Column("rp_5", sa.Float(), nullable=True),
        sa.Column("rp_10", sa.Float(), nullable=True),
        sa.Column("rp_25", sa.Float(), nullable=True),
        sa.Column("rp_50", sa.Float(), nullable=True),
        sa.Column("rp_100", sa.Float(), nullable=True),
        sa.Column("metadata_json", sa.JSON(), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint("provider", "provider_reach_id", name="uq_rp_provider_reach"),
    )
    op.create_index(op.f("ix_forecast_provider_return_periods_provider"), "forecast_provider_return_periods", ["provider"])
    op.create_index(op.f("ix_forecast_provider_return_periods_provider_reach_id"), "forecast_provider_return_periods", ["provider_reach_id"])

    op.create_table(
        "forecast_provider_reach_timeseries",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("provider", sa.String(length=64), nullable=False),
        sa.Column("run_id", sa.String(length=128), nullable=False),
        sa.Column("provider_reach_id", sa.String(length=128), nullable=False),
        sa.Column("forecast_time_utc", sa.DateTime(timezone=True), nullable=False),
        sa.Column("flow_mean_cms", sa.Float(), nullable=True),
        sa.Column("flow_median_cms", sa.Float(), nullable=True),
        sa.Column("flow_p25_cms", sa.Float(), nullable=True),
        sa.Column("flow_p75_cms", sa.Float(), nullable=True),
        sa.Column("flow_max_cms", sa.Float(), nullable=True),
        sa.Column("raw_payload_json", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint("provider", "run_id", "provider_reach_id", "forecast_time_utc", name="uq_ts_provider_run_reach_time"),
    )
    op.create_index("idx_ts_provider_reach", "forecast_provider_reach_timeseries", ["provider", "provider_reach_id"])
    op.create_index("idx_ts_provider_run", "forecast_provider_reach_timeseries", ["provider", "run_id"])

    op.create_table(
        "forecast_provider_reach_summaries",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("provider", sa.String(length=64), nullable=False),
        sa.Column("run_id", sa.String(length=128), nullable=False),
        sa.Column("provider_reach_id", sa.String(length=128), nullable=False),
        sa.Column("peak_time_utc", sa.DateTime(timezone=True), nullable=True),
        sa.Column("first_exceedance_time_utc", sa.DateTime(timezone=True), nullable=True),
        sa.Column("peak_mean_cms", sa.Float(), nullable=True),
        sa.Column("peak_median_cms", sa.Float(), nullable=True),
        sa.Column("peak_max_cms", sa.Float(), nullable=True),
        sa.Column("return_period_band", sa.String(length=16), nullable=True),
        sa.Column("severity_score", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("is_flagged", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("metadata_json", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint("provider", "run_id", "provider_reach_id", name="uq_summary_provider_run_reach"),
    )
    op.create_index(op.f("ix_forecast_provider_reach_summaries_provider"), "forecast_provider_reach_summaries", ["provider"])
    op.create_index(op.f("ix_forecast_provider_reach_summaries_provider_reach_id"), "forecast_provider_reach_summaries", ["provider_reach_id"])
    op.create_index(op.f("ix_forecast_provider_reach_summaries_run_id"), "forecast_provider_reach_summaries", ["run_id"])


def downgrade() -> None:
    op.drop_table("forecast_provider_reach_summaries")
    op.drop_table("forecast_provider_reach_timeseries")
    op.drop_table("forecast_provider_return_periods")
    op.drop_table("forecast_runs")
