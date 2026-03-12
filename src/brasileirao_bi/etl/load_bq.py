import json
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
from google.cloud import bigquery

from brasileirao_bi.bq.client import get_bq_client, table_id

RAW_DIR = Path("data/raw")
DEFAULT_SEASONS = [2023, 2024, 2025, 2026]
DEFAULT_COMPETITION = "BSA"


def load_replace(client: bigquery.Client, df: pd.DataFrame, full_table_id: str) -> None:
    if df.empty:
        return
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    job = client.load_table_from_dataframe(df, full_table_id, job_config=job_config)
    job.result()


def to_match_date(utc_date_series: pd.Series) -> pd.Series:
    return pd.to_datetime(utc_date_series, utc=True).dt.date


def _load_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _ensure_table(client: bigquery.Client, full_table_id: str, schema: list[bigquery.SchemaField]) -> None:
    try:
        existing = client.get_table(full_table_id)
        existing_fields = [f.name for f in existing.schema]
        desired_fields = [f.name for f in schema]

        # Se já existe mas não bate, derruba e recria
        if existing_fields != desired_fields:
            client.delete_table(full_table_id, not_found_ok=True)
            table = bigquery.Table(full_table_id, schema=schema)
            client.create_table(table, exists_ok=True)
        return
    except Exception:
        table = bigquery.Table(full_table_id, schema=schema)
        client.create_table(table, exists_ok=True)


def _teams_rows(teams_payload: Dict[str, Any], season: int) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for t in teams_payload.get("teams", []) or []:
        rows.append(
            {
                "team_id": t.get("id"),
                "season": season,
                "team_name": t.get("name"),
                "team_short_name": t.get("shortName"),
                "team_abbr": t.get("tla"),
            }
        )
    return rows


def _matches_rows(matches_payload: Dict[str, Any], season: int) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for m in matches_payload.get("matches", []) or []:
        home = m.get("homeTeam") or {}
        away = m.get("awayTeam") or {}
        ft = ((m.get("score") or {}).get("fullTime") or {})
        rows.append(
            {
                "match_id": m.get("id"),
                "season": season,
                "matchday": m.get("matchday"),
                "utc_date": m.get("utcDate"),
                "status": m.get("status"),
                "home_team_id": home.get("id"),
                "away_team_id": away.get("id"),
                "goals_home": ft.get("home"),
                "goals_away": ft.get("away"),
            }
        )
    return rows


def load_raw_tables(
    seasons: list[int] | None = None,
    competition: str = DEFAULT_COMPETITION,
) -> None:
    """
    Lê os JSONs em data/raw e carrega:
      - raw_teams   (teams do campeonato por temporada)
      - raw_matches (todas as partidas do campeonato por temporada)
    """
    if seasons is None:
        seasons = DEFAULT_SEASONS

    client = get_bq_client()

    team_rows: List[Dict[str, Any]] = []
    match_rows: List[Dict[str, Any]] = []

    for season in seasons:
        teams_path = RAW_DIR / f"teams_{competition}_{season}.json"
        matches_path = RAW_DIR / f"matches_{competition}_{season}.json"

        if not teams_path.exists():
            print(f"[SKIP] {teams_path} não existe.")
        else:
            teams_data = _load_json(teams_path)
            team_rows.extend(_teams_rows(teams_data, season))

        if not matches_path.exists():
            print(f"[SKIP] {matches_path} não existe.")
        else:
            matches_data = _load_json(matches_path)
            match_rows.extend(_matches_rows(matches_data, season))

    df_teams = pd.DataFrame(team_rows)
    df_matches = pd.DataFrame(match_rows)

    if df_teams.empty and df_matches.empty:
        print("Nada para carregar (raw_teams e raw_matches vazios).")
        return

    # Tipos coerentes
    if not df_teams.empty:
        df_teams["team_id"] = pd.to_numeric(df_teams["team_id"], errors="coerce").astype("Int64")
        df_teams["season"] = pd.to_numeric(df_teams["season"], errors="coerce").astype("Int64")
        df_teams["team_name"] = df_teams["team_name"].astype("string")
        df_teams["team_short_name"] = df_teams["team_short_name"].astype("string")
        df_teams["team_abbr"] = df_teams["team_abbr"].astype("string")

        # Dedup por team_id (mantém o primeiro não-nulo; evita duplicar entre seasons)
        df_teams = (
            df_teams.sort_values(["team_id", "season"])
            .drop_duplicates(subset=["team_id"], keep="last")
            .reset_index(drop=True)
        )

    if not df_matches.empty:
        df_matches["utc_date"] = pd.to_datetime(df_matches["utc_date"], utc=True, errors="coerce")
        df_matches["match_date"] = to_match_date(df_matches["utc_date"])
        df_matches["match_id"] = pd.to_numeric(df_matches["match_id"], errors="coerce").astype("Int64")
        df_matches["season"] = pd.to_numeric(df_matches["season"], errors="coerce").astype("Int64")
        df_matches["matchday"] = pd.to_numeric(df_matches["matchday"], errors="coerce").astype("Int64")
        df_matches["home_team_id"] = pd.to_numeric(df_matches["home_team_id"], errors="coerce").astype("Int64")
        df_matches["away_team_id"] = pd.to_numeric(df_matches["away_team_id"], errors="coerce").astype("Int64")
        df_matches["goals_home"] = pd.to_numeric(df_matches["goals_home"], errors="coerce").astype("Int64")
        df_matches["goals_away"] = pd.to_numeric(df_matches["goals_away"], errors="coerce").astype("Int64")
        df_matches["status"] = df_matches["status"].astype("string")

    # --- schemas ---
    teams_schema = [
        bigquery.SchemaField("team_id", "INT64"),
        bigquery.SchemaField("season", "INT64"),
        bigquery.SchemaField("team_name", "STRING"),
        bigquery.SchemaField("team_short_name", "STRING"),
        bigquery.SchemaField("team_abbr", "STRING"),
    ]

    matches_schema = [
        bigquery.SchemaField("match_id", "INT64"),
        bigquery.SchemaField("season", "INT64"),
        bigquery.SchemaField("matchday", "INT64"),
        bigquery.SchemaField("utc_date", "TIMESTAMP"),
        bigquery.SchemaField("match_date", "DATE"),
        bigquery.SchemaField("status", "STRING"),
        bigquery.SchemaField("home_team_id", "INT64"),
        bigquery.SchemaField("away_team_id", "INT64"),
        bigquery.SchemaField("goals_home", "INT64"),
        bigquery.SchemaField("goals_away", "INT64"),
    ]

    t_teams = table_id("raw_teams")
    t_matches = table_id("raw_matches")

    _ensure_table(client, t_teams, teams_schema)
    _ensure_table(client, t_matches, matches_schema)

    print("\n=== Criando camada RAW ===\n")

    if not df_teams.empty:
        load_replace(client, df_teams, t_teams)
        print(f"OK: {t_teams} | rows={len(df_teams)}")

    if not df_matches.empty:
        load_replace(client, df_matches, t_matches)
        print(f"OK: {t_matches} | rows={len(df_matches)}")
    
    print("\n✅ Camada raw criada com sucesso\n")