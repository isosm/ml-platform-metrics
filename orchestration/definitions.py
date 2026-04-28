"""
Dagster definitions — assets, schedule, and resources.

Asset lineage:
  raw_github_events ──┐
                       ├──► dbt (staging → intermediate → marts) ──► platform_health_report
  raw_ml_events ───────┘

Schedule: daily at 06:00 UTC.

Environment control: DBT_TARGET env var selects dev or prod BigQuery datasets.
  dev  → dbt_dev_* datasets  (default)
  prod → staging, marts, etc.
"""

import os
from pathlib import Path

from dagster import (
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    load_assets_from_modules,
)
from dagster_dbt import DbtCliResource, dbt_assets

from orchestration.assets import raw_data, platform_report

PROJECT_ROOT    = Path(__file__).parent.parent
DBT_PROJECT_DIR = PROJECT_ROOT / "dbt"
DBT_TARGET      = os.environ.get("DBT_TARGET", "dev")
MANIFEST_PATH   = DBT_PROJECT_DIR / "target" / "manifest.json"

raw_assets    = load_assets_from_modules([raw_data], group_name="raw")
report_assets = load_assets_from_modules([platform_report], group_name="reporting")


@dbt_assets(manifest=MANIFEST_PATH)
def ml_platform_dbt_assets(context, dbt: DbtCliResource):
    yield from dbt.cli(
        ["build", "--target", DBT_TARGET],
        context=context,
    ).stream()


daily_pipeline = define_asset_job(
    name="daily_platform_pipeline",
    selection="*",
    description="Full DORA-for-ML pipeline: generate raw data → dbt → health report.",
)

daily_schedule = ScheduleDefinition(
    name="daily_platform_schedule",
    cron_schedule="0 6 * * *",
    job=daily_pipeline,
    execution_timezone="UTC",
)

dbt_resource = DbtCliResource(
    project_dir=os.fspath(DBT_PROJECT_DIR),
    profiles_dir=os.fspath(DBT_PROJECT_DIR),
)

defs = Definitions(
    assets=[*raw_assets, ml_platform_dbt_assets, *report_assets],
    schedules=[daily_schedule],
    resources={"dbt": dbt_resource},
)
