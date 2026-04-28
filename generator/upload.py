"""BigQuery upload — raw layer."""

import pandas as pd
from google.cloud import bigquery

PROJECT_ID = "ml-platform-metrics-494708"
DATASET_ID = "raw"
LOCATION   = "europe-north1"

SCHEMAS: dict[str, list[bigquery.SchemaField]] = {
    "github_events": [
        bigquery.SchemaField("event_id",       "STRING",    mode="REQUIRED"),
        bigquery.SchemaField("team",           "STRING",    mode="REQUIRED"),
        bigquery.SchemaField("event_type",     "STRING",    mode="REQUIRED"),
        bigquery.SchemaField("event_date",     "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("lead_time_days", "FLOAT",     mode="NULLABLE"),
        bigquery.SchemaField("ci_passed",      "BOOLEAN",   mode="NULLABLE"),
    ],
    "ml_events": [
        bigquery.SchemaField("event_id",        "STRING",    mode="REQUIRED"),
        bigquery.SchemaField("team",            "STRING",    mode="REQUIRED"),
        bigquery.SchemaField("model_name",      "STRING",    mode="REQUIRED"),
        bigquery.SchemaField("event_type",      "STRING",    mode="REQUIRED"),
        bigquery.SchemaField("event_date",      "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("psi_score",       "FLOAT",     mode="NULLABLE"),
        bigquery.SchemaField("precision_at_10", "FLOAT",     mode="NULLABLE"),
        bigquery.SchemaField("drift_triggered", "BOOLEAN",   mode="NULLABLE"),
    ],
}


def ensure_dataset(client: bigquery.Client) -> None:
    ds = bigquery.Dataset(f"{PROJECT_ID}.{DATASET_ID}")
    ds.location = LOCATION
    client.create_dataset(ds, exists_ok=True)
    print(f"Dataset `{PROJECT_ID}.{DATASET_ID}` ready.")


def upload_table(client: bigquery.Client, table_name: str, df: pd.DataFrame) -> None:
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
    job_config = bigquery.LoadJobConfig(
        schema=SCHEMAS[table_name],
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    table = client.get_table(table_id)
    print(f"  {table.num_rows:>6,} rows → {table_id}")


def upload_all(dataframes: dict[str, pd.DataFrame]) -> None:
    client = bigquery.Client(project=PROJECT_ID)
    ensure_dataset(client)
    print("\nUploading to BigQuery...")
    for name, df in dataframes.items():
        upload_table(client, name, df)
    print("\nDone.")
