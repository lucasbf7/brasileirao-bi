from __future__ import annotations

import os
from pathlib import Path
from google.cloud import bigquery

from brasileirao_bi.etl.transforms.mart import run_query

SQL_DIR = Path(__file__).resolve().parents[3] / "sql"


def read_sql(filename: str) -> str:
    return (SQL_DIR / filename).read_text(encoding="utf-8")


def create_view_from_file(client: bigquery.Client, filename: str, view_name: str) -> None:
    project_id = os.environ["GCP_PROJECT_ID"]
    dataset = os.environ["BQ_DATASET"]

    query = read_sql(filename).format(
        project_id=project_id,
        dataset=dataset,
    )
    run_query(client, query, view_name)


def run_views_layer(client: bigquery.Client) -> None:
    print("\n=== Criando views analíticas ===\n")

    create_view_from_file(client, "dim_teams.sql", "vw_dim_teams")
    create_view_from_file(client, "future_matches_next5_2026.sql", "vw_future_matches_next5_2026")
    create_view_from_file(client, "league_ppj_avg_2026.sql", "vw_league_ppj_avg_2026")
    create_view_from_file(client, "league_projection.sql", "vw_league_projection")
    create_view_from_file(client, "monte_carlo_matches_2026.sql", "vw_monte_carlo_matches_2026")
    create_view_from_file(client, "monte_carlo_summary_2026.sql", "vw_monte_carlo_summary_2026")
    create_view_from_file(client, "monte_carlo_neighbors_2026.sql", "vw_monte_carlo_neighbors_2026")
    create_view_from_file(client, "over_under_achievers_2026.sql", "vw_over_under_achievers_2026")
    create_view_from_file(client, "points_real_vs_projected_2026.sql", "vw_points_real_vs_projected_2026")
    create_view_from_file(client, "projection_by_match_2026.sql", "vw_projection_by_match_2026")
    create_view_from_file(client, "team_form.sql", "vw_team_form")
    create_view_from_file(client, "team_trend_2026.sql", "vw_team_trend_2026")
    create_view_from_file(client, "team_trend_matchday.sql", "vw_team_trend_matchday")
    create_view_from_file(client, "team_trend_matchday_2026.sql", "vw_team_trend_matchday_2026")

    print("\n✅ Views analíticas criadas com sucesso\n")