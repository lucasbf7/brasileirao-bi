from __future__ import annotations
import os
from google.cloud import bigquery

def get_bq_client() -> bigquery.Client:
    project_id = os.getenv("GCP_PROJECT_ID")
    location = os.getenv("BQ_LOCATION", "southamerica-east1")
    if project_id:
        return bigquery.Client(project=project_id, location=location)
    return bigquery.Client(location=location)

def table_id(table_name: str) -> str:
    project_id = os.getenv("GCP_PROJECT_ID")
    if not project_id:
        raise RuntimeError("Missing GCP_PROJECT_ID env var.")
    dataset = os.getenv("BQ_DATASET", "brasileirao_bi")
    return f"{project_id}.{dataset}.{table_name}"