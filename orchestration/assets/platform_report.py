"""
Platform health report asset.

Läser fct_platform_health_daily och loggar en sammanfattning.
I produktion pushar detta till Slack eller PagerDuty.

Dataset väljs baserat på DBT_TARGET-miljövariabeln:
  dev  → dbt_dev_marts  (lokala dev-körningar)
  prod → marts           (produktionsdata)

GCP-ekvivalent: Cloud Run-jobb triggas av Pub/Sub efter dbt-körning.
"""

import os
from dagster import asset, AssetExecutionContext, Output, MetadataValue

DBT_TARGET = os.environ.get("DBT_TARGET", "dev")
DATASET    = "dbt_dev_marts" if DBT_TARGET != "prod" else "marts"
PROJECT    = "ml-platform-metrics-494708"


@asset(
    group_name="reporting",
    description="Läser fct_platform_health_daily och rapporterar kritiska hälsovarningar.",
    compute_kind="python",
    deps=["raw_github_events", "raw_ml_events"],
)
def platform_health_report(context: AssetExecutionContext) -> Output[dict]:
    from google.cloud import bigquery

    client = bigquery.Client(project=PROJECT)

    query = f"""
        SELECT
            team,
            date_day,
            health_score,
            health_tier,
            ROUND(ci_failure_rate_7d_avg * 100, 1)   AS ci_failure_pct,
            ROUND(drift_failure_rate_7d_avg * 100, 1) AS drift_failure_pct,
            ROUND(avg_mttr_hours, 1)                  AS mttr_hours
        FROM `{PROJECT}.{DATASET}.fct_platform_health_daily`
        WHERE date_day = (
            SELECT MAX(date_day)
            FROM `{PROJECT}.{DATASET}.fct_platform_health_daily`
        )
        ORDER BY health_score ASC
    """

    df = client.query(query).to_dataframe()

    critical = df[df["health_tier"] == "critical"]
    warning  = df[df["health_tier"] == "warning"]
    healthy  = df[df["health_tier"] == "healthy"]

    if not critical.empty:
        context.log.warning(
            f"CRITICAL — {len(critical)} team(s) under hälsotröskel 70:\n"
            + critical[["team", "health_score"]].to_string(index=False)
        )
    if not warning.empty:
        context.log.warning(f"WARNING — {len(warning)} team(s) i varningszon (70–90).")

    context.log.info(f"Friska team: {len(healthy)}/4")

    scores = df.set_index("team")["health_score"].round(1).to_dict()
    context.log.info(f"Hälsoscorer: {scores}")

    return Output(
        value=scores,
        metadata={
            "dataset":        MetadataValue.text(f"{PROJECT}.{DATASET}"),
            "critical_teams": MetadataValue.int(len(critical)),
            "warning_teams":  MetadataValue.int(len(warning)),
            "healthy_teams":  MetadataValue.int(len(healthy)),
            "scores":         MetadataValue.json(scores),
        },
    )
