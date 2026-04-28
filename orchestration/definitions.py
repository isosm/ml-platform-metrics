"""
Dagster definitions — asset-graf, schema och jobb.

Varför Dagster och inte Airflow?
  Airflow tänker i tasks: "kör detta, sen detta, sen detta."
  Dagster tänker i assets: "detta data-objekt beror på det data-objektet."
  Det ger Dagster en visuell lineage-graf som visar exakt vad som
  producerar vad — precis som dbt's DAG men för hela pipelinen inklusive
  Python-koden som genererar rådata.

Asset-linjegraf:
  raw_github_events ──┐
                       ├──► dbt staging → intermediate → marts ──► platform_health_report
  raw_ml_events ───────┘

Miljöhantering:
  DBT_TARGET=dev  (default) → dev-datasets i BigQuery (dbt_dev_*)
  DBT_TARGET=prod           → prod-datasets i BigQuery (staging, marts etc.)
"""

import os
from pathlib import Path

from dagster import (
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    load_assets_from_modules,
)
from dagster_dbt import DbtProject, DbtCliResource, dbt_assets

from orchestration.assets import raw_data, platform_report

# ---------------------------------------------------------------------------
# Sökvägar
# ---------------------------------------------------------------------------

PROJECT_ROOT   = Path(__file__).parent.parent
DBT_PROJECT_DIR = PROJECT_ROOT / "dbt"
DBT_TARGET      = os.environ.get("DBT_TARGET", "dev")

# ---------------------------------------------------------------------------
# Raw data assets  (generator → BigQuery raw)
# ---------------------------------------------------------------------------

raw_assets = load_assets_from_modules([raw_data], group_name="raw")

# ---------------------------------------------------------------------------
# dbt assets
#
# DbtProject pekar på manifest.json (genererad av `dbt parse`).
# Dagster läser manifestet och skapar ett asset per dbt-modell — med
# korrekt beroendegraf, beskrivningar och taggar från dbt-projektet.
# I prod används DbtProject.prepare_if_dev() för att auto-parsa vid behov.
# ---------------------------------------------------------------------------

# Pekar direkt på pre-byggt manifest.json (genererat av `dbt parse`).
# Fördelar: laddar snabbt, kräver inga BigQuery-credentials vid startup,
# och är exakt vad man gör i produktion (manifest genereras i CI).
MANIFEST_PATH = DBT_PROJECT_DIR / "target" / "manifest.json"


@dbt_assets(manifest=MANIFEST_PATH)
def ml_platform_dbt_assets(context, dbt: DbtCliResource):
    """Kör hela dbt-projektet. Varje modell är ett Dagster-asset i lineage-grafen."""
    yield from dbt.cli(
        ["build", "--target", DBT_TARGET],
        context=context,
    ).stream()


# ---------------------------------------------------------------------------
# Reporting assets  (läser marts → loggar hälsostatus)
# ---------------------------------------------------------------------------

report_assets = load_assets_from_modules([platform_report], group_name="reporting")

# ---------------------------------------------------------------------------
# Jobb — hela dagliga pipelinen
# ---------------------------------------------------------------------------

daily_pipeline = define_asset_job(
    name="daily_platform_pipeline",
    selection="*",
    description="Full DORA-for-ML pipeline: generera rådata → dbt → rapport.",
)

# ---------------------------------------------------------------------------
# Schema — kör kl 06:00 UTC varje dag
# Cron-syntax: minut timme dag månad veckodag
# "0 6 * * *" = kl 06:00, varje dag, varje månad, alla veckodagar
# ---------------------------------------------------------------------------

daily_schedule = ScheduleDefinition(
    name="daily_platform_schedule",
    cron_schedule="0 6 * * *",
    job=daily_pipeline,
    execution_timezone="UTC",
)

# ---------------------------------------------------------------------------
# Resources — dbt CLI konfigurerad med korrekt target
# ---------------------------------------------------------------------------

dbt_resource = DbtCliResource(
    project_dir=os.fspath(DBT_PROJECT_DIR),
    profiles_dir=os.fspath(DBT_PROJECT_DIR),
)

# ---------------------------------------------------------------------------
# Definitions — enda entry point som Dagster läser
# ---------------------------------------------------------------------------

defs = Definitions(
    assets=[*raw_assets, ml_platform_dbt_assets, *report_assets],
    schedules=[daily_schedule],
    resources={"dbt": dbt_resource},
)
