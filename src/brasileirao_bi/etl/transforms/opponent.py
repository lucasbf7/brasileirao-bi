from __future__ import annotations

from google.cloud import bigquery

from brasileirao_bi.bq.client import table_id
from brasileirao_bi.etl.transforms.mart import run_query


def create_agg_opponent_score_23_25(client: bigquery.Client, k_prior: int = 3) -> None:
    """
    Score de "dificuldade" (na prática, o quanto um time costuma pontuar vs um adversário)
    com shrinkage usando prior por (team_id, is_home) vindo do agg_hist_local_23_25.
    """
    query = f"""
    CREATE OR REPLACE TABLE `{table_id("agg_opponent_score_23_25")}` AS
    WITH base AS (
      SELECT
        team_id,
        opponent_team_id,
        opp_team_name,
        opp_team_abbr,
        is_home,

        COUNTIF(is_played = 1) AS jogos,
        SUM(CASE WHEN result='W' THEN 1 ELSE 0 END) AS vitorias,
        SUM(CASE WHEN result='D' THEN 1 ELSE 0 END) AS empates,
        SUM(CASE WHEN result='L' THEN 1 ELSE 0 END) AS derrotas,
        SUM(COALESCE(points,0)) AS pontos,
        SUM(COALESCE(goal_diff,0)) AS saldo_gols
      FROM `{table_id("fact_matches_running")}`
      WHERE season IN (2023, 2024, 2025)
        AND is_played = 1
      GROUP BY 1,2,3,4,5
    ),

    fallback AS (
      SELECT
        team_id,
        is_home,
        ppj_hist_fallback_local AS prior_ppj
      FROM `{table_id("agg_hist_local_23_25")}`
    ),

    calc AS (
      SELECT
        b.*,
        f.prior_ppj,

        SAFE_DIVIDE(pontos, jogos) AS ppj_raw,

        SAFE_DIVIDE(
          pontos + (COALESCE(f.prior_ppj, 0.0) * {k_prior}),
          jogos + {k_prior}
        ) AS ppj_shrink
      FROM base b
      LEFT JOIN fallback f
        ON f.team_id = b.team_id
       AND f.is_home = b.is_home
    )

    SELECT
      *,
      SAFE_DIVIDE(ppj_shrink, 3.0) AS score_facilidade,
      1 - SAFE_DIVIDE(ppj_shrink, 3.0) AS score_dificuldade,
      CASE
        WHEN ppj_shrink >= 2.0 THEN 'Freguês'
        WHEN ppj_shrink <= 1.0 THEN 'Carrasco'
        ELSE 'Neutro'
      END AS categoria
    FROM calc
    """
    run_query(client, query, "agg_opponent_score_23_25")