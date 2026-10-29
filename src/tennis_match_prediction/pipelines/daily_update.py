"""Dagster component for the daily history refresh and forecast evaluation workflow."""

from pathlib import Path

import dagster as dg


class DailyUpdateConfig(dg.Config):
    years: list[int] | None = None
    csv_path: str | None = None
    curated_dir: str | None = None
    predictions_dir: str | None = None
    performance_dir: str | None = None
    local_only: bool = False
    include_ongoing: bool = True
    max_requests: int = 20


class DailyUpdate(dg.Component, dg.Resolvable, dg.Model):
    """Wrap bounded REST collection; definitions perform no network or disk access."""

    def build_defs(self, context: dg.ComponentLoadContext | None = None) -> dg.Definitions:
        @dg.op(out={})
        def update_history_and_performance(
            context: dg.OpExecutionContext, config: DailyUpdateConfig
        ):
            from tennis_match_prediction.workflows.daily_update import (
                DailyUpdateSettings,
                run_daily_update,
            )

            options = config.model_dump(exclude_none=True)
            for key in ("csv_path", "curated_dir", "predictions_dir", "performance_dir"):
                if key in options:
                    options[key] = Path(options[key])
            report = run_daily_update(DailyUpdateSettings(**options))
            context.log.info("Daily update completed: %s", report)

        @dg.job(
            description="Refresh validated history and evaluate archived forecasts; no training"
        )
        def daily_update():
            update_history_and_performance()  # pylint: disable=no-value-for-parameter

        schedule = dg.ScheduleDefinition(
            name="daily_history_and_performance",
            job=daily_update,
            cron_schedule="0 6 * * *",
            execution_timezone="America/Sao_Paulo",
            default_status=dg.DefaultScheduleStatus.RUNNING,
            description="Daily at 06:00 São Paulo; requires a running Dagster daemon.",
        )
        return dg.Definitions(jobs=[daily_update], schedules=[schedule])
