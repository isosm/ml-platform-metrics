"""
Raw data assets — run the synthetic generator and load to BigQuery.

In production this would be replaced by a real data ingestion layer
(GitHub API via dlt, ML platform event bus, etc.). The generator acts as
a stand-in that produces the same schema, so the dbt models are production-ready.

GCP equivalent: Cloud Composer DAG or Dataflow job writing to BigQuery.
"""

from dagster import asset, AssetExecutionContext, Output, MetadataValue


@asset(
    group_name="raw",
    description="Synthetic GitHub delivery events (DORA signals) loaded to BigQuery raw.github_events.",
    compute_kind="python",
)
def raw_github_events(context: AssetExecutionContext) -> Output[int]:
    from generator.generate import generate_github_events
    from generator.upload import upload_table
    from google.cloud import bigquery

    context.log.info("Generating GitHub events...")
    df = generate_github_events()

    context.log.info(f"Generated {len(df):,} events. Uploading to BigQuery...")
    client = bigquery.Client(project="ml-platform-metrics-494708")
    upload_table(client, "github_events", df)

    event_breakdown = df.groupby("event_type").size().to_dict()
    context.log.info(f"Breakdown: {event_breakdown}")

    return Output(
        value=len(df),
        metadata={
            "row_count":       MetadataValue.int(len(df)),
            "event_breakdown": MetadataValue.json(event_breakdown),
            "table":           MetadataValue.text("ml-platform-metrics-494708.raw.github_events"),
        },
    )


@asset(
    group_name="raw",
    description="Synthetic ML lifecycle events (ML-DORA signals) loaded to BigQuery raw.ml_events.",
    compute_kind="python",
)
def raw_ml_events(context: AssetExecutionContext) -> Output[int]:
    from generator.generate import generate_ml_events
    from generator.upload import upload_table
    from google.cloud import bigquery

    context.log.info("Generating ML events...")
    df = generate_ml_events()

    context.log.info(f"Generated {len(df):,} events. Uploading to BigQuery...")
    client = bigquery.Client(project="ml-platform-metrics-494708")
    upload_table(client, "ml_events", df)

    event_breakdown = df.groupby("event_type").size().to_dict()

    return Output(
        value=len(df),
        metadata={
            "row_count":        MetadataValue.int(len(df)),
            "event_breakdown":  MetadataValue.json(event_breakdown),
            "table":            MetadataValue.text("ml-platform-metrics-494708.raw.ml_events"),
        },
    )
