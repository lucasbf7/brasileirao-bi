from __future__ import annotations

from brasileirao_bi.bq.client import table_id
from brasileirao_bi.etl.transforms.mart import run_query


def create_agg_form_2026(client) -> None:
    query = f"""
    CREATE OR REPLACE TABLE `{table_id("agg_form_2026")}` AS
    WITH played AS (
      SELECT *
      FROM `{table_id("fact_matches_running")}`
      WHERE season = 2026
        AND is_played = 1
    ),

    last_by_team_local AS (
      SELECT
        team_id,
        is_home,
        ppj_ultimos_5 AS ppj_last5_local
      FROM played
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY team_id, is_home
        ORDER BY matchday DESC, match_date DESC, match_id DESC
      ) = 1
    ),

    last_by_team_overall AS (
      SELECT
        team_id,
        ppj_ultimos_5 AS ppj_last5_overall,
        ppj_acumulado AS ppj_acumulado_overall,
        pontos_acumulados,
        jogos_disputados
      FROM played
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY team_id
        ORDER BY matchday DESC, match_date DESC, match_id DESC
      ) = 1
    )

    SELECT
      lbl.team_id,
      lbl.is_home,
      lbl.ppj_last5_local,
      lo.ppj_last5_overall,
      lo.ppj_acumulado_overall,
      lo.pontos_acumulados,
      lo.jogos_disputados
    FROM last_by_team_local lbl
    LEFT JOIN last_by_team_overall lo
      ON lo.team_id = lbl.team_id
    """
    run_query(client, query, "agg_form_2026")
  