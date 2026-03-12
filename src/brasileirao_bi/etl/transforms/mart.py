from __future__ import annotations

from brasileirao_bi.bq.client import table_id


def run_query(client, query: str, label: str) -> None:
    job = client.query(query)
    job.result()
    print(f"OK: {label}")


def create_matches_enriched(client) -> None:
    """
    Base canonica por time (1 jogo => 2 linhas), multi-temporada e incluindo futuros.
    Fonte: raw_matches + dim_teams_latest
    """
    query = f"""
    CREATE OR REPLACE TABLE `{table_id("matches_enriched")}` AS
    WITH m AS (
      SELECT
        match_id,
        season,
        matchday,
        match_date,
        utc_date,
        status,
        home_team_id,
        away_team_id,
        goals_home,
        goals_away
      FROM `{table_id("raw_matches")}`
    ),

    base AS (
      -- linha do mandante
      SELECT
        match_id, season, matchday, match_date, utc_date, status,
        home_team_id AS team_id,
        away_team_id AS opponent_team_id,
        TRUE AS is_home,
        goals_home AS goals_for,
        goals_away AS goals_against
      FROM m

      UNION ALL

      -- linha do visitante
      SELECT
        match_id, season, matchday, match_date, utc_date, status,
        away_team_id AS team_id,
        home_team_id AS opponent_team_id,
        FALSE AS is_home,
        goals_away AS goals_for,
        goals_home AS goals_against
      FROM m
    )

    SELECT
      b.*,

      (b.goals_for - b.goals_against) AS goal_diff,

      -- só existe se o jogo acabou (evita "data já passou => derrota" etc.)
      CASE WHEN b.status = 'FINISHED' THEN
        CASE
          WHEN b.goals_for > b.goals_against THEN 3
          WHEN b.goals_for = b.goals_against THEN 1
          ELSE 0
        END
      END AS points,

      CASE WHEN b.status = 'FINISHED' THEN
        CASE
          WHEN b.goals_for > b.goals_against THEN 'W'
          WHEN b.goals_for = b.goals_against THEN 'D'
          ELSE 'L'
        END
      END AS result,

      CASE WHEN b.status = 'FINISHED' THEN 1 ELSE 0 END AS is_played,
      CASE WHEN b.status = 'FINISHED' AND b.goals_against = 0 THEN 1 ELSE 0 END AS clean_sheet,

      t.team_name       AS team_name,
      t.team_short_name AS team_short_name,
      t.team_abbr       AS team_abbr,

      o.team_name       AS opp_team_name,
      o.team_short_name AS opp_team_short_name,
      o.team_abbr       AS opp_team_abbr

    FROM base b
    LEFT JOIN `{table_id("dim_teams_latest")}` t
      ON t.team_id = b.team_id
    LEFT JOIN `{table_id("dim_teams_latest")}` o
      ON o.team_id = b.opponent_team_id
    """
    run_query(client, query, "matches_enriched")


def create_fact_matches_running(client) -> None:
    query = f"""
    CREATE OR REPLACE TABLE `{table_id("fact_matches_running")}` AS
    WITH base AS (
      SELECT
        *,

        CASE WHEN matchday <= 19 THEN 'Turno' ELSE 'Returno' END AS turno_returno,
        CASE WHEN matchday <= 19 THEN 1 ELSE 2 END AS ord_turno_returno
      FROM `{table_id("matches_enriched")}`
    )
    SELECT
      base.*,

      SUM(COALESCE(points, 0)) OVER (
        PARTITION BY season, team_id
        ORDER BY matchday, match_date, match_id
      ) AS pontos_acumulados,

      SUM(COALESCE(goal_diff, 0)) OVER (
        PARTITION BY season, team_id
        ORDER BY matchday, match_date, match_id
      ) AS saldo_gols_acumulado,

      SUM(CASE WHEN is_played = 1 THEN 1 ELSE 0 END) OVER (
        PARTITION BY season, team_id
        ORDER BY matchday, match_date, match_id
      ) AS jogos_disputados,

      SAFE_DIVIDE(
        SUM(COALESCE(points, 0)) OVER (
          PARTITION BY season, team_id
          ORDER BY matchday, match_date, match_id
        ),
        NULLIF(
          SUM(CASE WHEN is_played = 1 THEN 1 ELSE 0 END) OVER (
            PARTITION BY season, team_id
            ORDER BY matchday, match_date, match_id
          ),
          0
        )
      ) AS ppj_acumulado,

      SAFE_DIVIDE(
        SUM(CASE WHEN is_played = 1 THEN COALESCE(points, 0) ELSE 0 END) OVER (
          PARTITION BY season, team_id
          ORDER BY matchday, match_date, match_id
          ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ),
        NULLIF(
          SUM(CASE WHEN is_played = 1 THEN 1 ELSE 0 END) OVER (
            PARTITION BY season, team_id
            ORDER BY matchday, match_date, match_id
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
          ),
          0
        )
      ) AS ppj_ultimos_5
    FROM base;
    """
    run_query(client, query, "fact_matches_running")