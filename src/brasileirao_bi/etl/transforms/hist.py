from __future__ import annotations

from brasileirao_bi.bq.client import table_id
from brasileirao_bi.etl.transforms.mart import run_query


def create_agg_hist_local_23_25(client) -> None:
    query = f"""
    CREATE OR REPLACE TABLE `{table_id("agg_hist_local_23_25")}` AS
    SELECT
      team_id,
      is_home,
      COUNTIF(is_played = 1) AS jogos,
      SAFE_DIVIDE(
        SUM(CASE WHEN is_played = 1 THEN COALESCE(points, 0) ELSE 0 END),
        NULLIF(COUNTIF(is_played = 1), 0)
      ) AS ppj_hist_fallback_local
    FROM `{table_id("fact_matches_running")}`
    WHERE season IN (2023, 2024, 2025)
    GROUP BY 1,2
    """
    run_query(client, query, "agg_hist_local_23_25")


def create_agg_hist_adv_local_23_25(client) -> None:
    query = f"""
    CREATE OR REPLACE TABLE `{table_id("agg_hist_adv_local_23_25")}` AS
    WITH base AS (
      SELECT
        f.team_id,
        f.opponent_team_id,
        f.opp_team_name,
        f.opp_team_abbr,
        f.is_home,
        f.is_played,
        f.result,
        f.points,
        f.goal_diff
      FROM `{table_id("fact_matches_running")}` f
      WHERE f.season IN (2023, 2024, 2025)
    )

    SELECT
      b.team_id,
      b.opponent_team_id,
      b.opp_team_name,
      b.opp_team_abbr,
      b.is_home,

      COUNTIF(b.is_played = 1) AS jogos,

      SUM(CASE WHEN b.is_played=1 AND b.result='W' THEN 1 ELSE 0 END) AS vitorias,
      SUM(CASE WHEN b.is_played=1 AND b.result='D' THEN 1 ELSE 0 END) AS empates,
      SUM(CASE WHEN b.is_played=1 AND b.result='L' THEN 1 ELSE 0 END) AS derrotas,

      SAFE_DIVIDE(
        SUM(CASE WHEN b.is_played=1 THEN COALESCE(b.points,0) ELSE 0 END),
        NULLIF(COUNTIF(b.is_played=1), 0)
      ) AS ppj_hist_adv_local,

      SAFE_DIVIDE(
        SUM(CASE WHEN b.is_played=1 THEN COALESCE(b.goal_diff,0) ELSE 0 END),
        NULLIF(COUNTIF(b.is_played=1), 0)
      ) AS saldo_gols_hist_adv_local

    FROM base b
    GROUP BY
      b.team_id,
      b.opponent_team_id,
      b.opp_team_name,
      b.opp_team_abbr,
      b.is_home
    """
    run_query(client, query, "agg_hist_adv_local_23_25")