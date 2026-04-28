from dagster import asset, AssetExecutionContext, Output, MetadataValue


@asset(
    group_name="raw",
    description="Synthetic GitHub delivery events loaded to BigQuery raw.github_events.",
    compute_kind="python",
)
def raw_github_events(context: AssetExecutionContext) -> Output[int]:
    from generator.generate import generate_github_events
    from generator.upload import upload_table
    from google.cloud import bigquery

    context.log.info("Generating GitHub events...")
    df = generate_github_events()

    client = bigquery.Client(project="ml-platform-metrics-494708")
    upload_table(client, "github_events", df)

    breakdown = df.groupby("event_type").size().to_dict()

    return Output(
        value=len(df),
        metadata={
            "row_count":       MetadataValue.int(len(df)),
            "event_breakdown": MetadataValue.json(breakdown),
            "table":           MetadataValue.text("ml-platform-metrics-494708.raw.github_events"),
        },
    )


@asset(
    group_name="raw",
    description="Synthetic ML lifecycle events loaded to BigQuery raw.ml_events.",
    compute_kind="python",
)
def raw_ml_events(context: AssetExecutionContext) -> Output[int]:
    from generator.generate import generate_ml_events
    from generator.upload import upload_table
    from google.cloud import bigquery

    context.log.info("Generating ML events...")
    df = generate_ml_events()

    client = bigquery.Client(project="ml-platform-metrics-494708")
    upload_table(client, "ml_events", df)

    breakdown = df.groupby("event_type").size().to_dict()

    return Output(
        value=len(df),
        metadata={
            "row_count":        MetadataValue.int(len(df)),
            "event_breakdown":  MetadataValue.json(breakdown),
            "table":            MetadataValue.text("ml-platform-metrics-494708.raw.ml_events"),
        },
    )
