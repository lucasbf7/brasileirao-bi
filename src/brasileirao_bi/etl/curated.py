from __future__ import annotations

from google.cloud import bigquery
from brasileirao_bi.bq.client import table_id
from brasileirao_bi.etl.transforms.mart import run_query


def create_cur_team_overrides(client: bigquery.Client) -> None:
    """
    Tabela pequena com correções manuais de nomes/siglas.
    """
    query = f"""
    CREATE OR REPLACE TABLE `{table_id("cur_team_overrides")}` AS
    SELECT * FROM UNNEST([
      STRUCT(1780 AS team_id, 'Vasco' AS team_short_name, 'VAS' AS team_abbr),
      STRUCT(1766 AS team_id, 'Atlético Mineiro' AS team_short_name, 'CAM' AS team_abbr),
      STRUCT(1768 AS team_id, 'Athletico Paranaense' AS team_short_name, 'CAP' AS team_abbr),
      STRUCT(4287 AS team_id, 'Remo' AS team_short_name, 'REM' AS team_abbr),

      STRUCT(6684 AS team_id, 'Internacional' AS team_short_name, 'INT' AS team_abbr),
      STRUCT(1776 AS team_id, 'São Paulo' AS team_short_name, 'SAO' AS team_abbr),
      STRUCT(1767 AS team_id, 'Grêmio' AS team_short_name, 'GRE' AS team_abbr),

      STRUCT(1838 AS team_id, 'América-MG' AS team_short_name, 'AME' AS team_abbr),
      STRUCT(3988 AS team_id, 'Atlético-GO' AS team_short_name, 'ACG' AS team_abbr),
      STRUCT(1778 AS team_id, 'Sport' AS team_short_name, 'SPO' AS team_abbr),
      STRUCT(1837 AS team_id, 'Ceará' AS team_short_name, 'CEA' AS team_abbr),
      STRUCT(3984 AS team_id, 'Fortaleza' AS team_short_name, 'FOR' AS team_abbr),
      STRUCT(4241 AS team_id, 'Coritiba' AS team_short_name, 'CFC' AS team_abbr)
    ])
    """
    run_query(client, query, "cur_team_overrides")


def create_cur_matches_2026(client):
    query = f"""
    CREATE OR REPLACE TABLE `{table_id("cur_matches_2026")}` AS
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
    WHERE season = 2026
    """
    run_query(client, query, "cur_matches_2026")


def create_cur_matches_hist(client):
    query = f"""
    CREATE OR REPLACE TABLE `{table_id("cur_matches_hist_23_25")}` AS
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
    WHERE season IN (2023, 2024, 2025)
    """
    run_query(client, query, "cur_matches_hist_23_25")


def create_cur_team_results_2026(client):
    query = f"""
    CREATE OR REPLACE TABLE `{table_id("cur_team_match_results_2026")}` AS
    WITH base AS (
        SELECT *
        FROM `{table_id("cur_matches_2026")}`
        -- sem filtro (ALL)
    )

    SELECT
        match_id,
        season,
        matchday,
        match_date,
        status,

        home_team_id AS team_id,
        away_team_id AS opponent_team_id,
        TRUE AS is_home,

        goals_home AS goals_for,
        goals_away AS goals_against,

        CASE WHEN status='FINISHED' THEN (goals_home - goals_away) ELSE NULL END AS goal_diff,

        CASE
            WHEN status!='FINISHED' THEN NULL
            WHEN goals_home > goals_away THEN 3
            WHEN goals_home = goals_away THEN 1
            ELSE 0
        END AS points,

        CASE
            WHEN status!='FINISHED' THEN NULL
            WHEN goals_home > goals_away THEN 'W'
            WHEN goals_home = goals_away THEN 'D'
            ELSE 'L'
        END AS result
    FROM base

    UNION ALL

    SELECT
        match_id,
        season,
        matchday,
        match_date,
        status,

        away_team_id AS team_id,
        home_team_id AS opponent_team_id,
        FALSE AS is_home,

        goals_away AS goals_for,
        goals_home AS goals_against,

        CASE WHEN status='FINISHED' THEN (goals_away - goals_home) ELSE NULL END AS goal_diff,

        CASE
            WHEN status!='FINISHED' THEN NULL
            WHEN goals_away > goals_home THEN 3
            WHEN goals_away = goals_home THEN 1
            ELSE 0
        END AS points,

        CASE
            WHEN status!='FINISHED' THEN NULL
            WHEN goals_away > goals_home THEN 'W'
            WHEN goals_away = goals_home THEN 'D'
            ELSE 'L'
        END AS result
    FROM base
    """
    run_query(client, query, "cur_team_match_results_2026")


def create_cur_team_results_hist(client):
    query = f"""
    CREATE OR REPLACE TABLE `{table_id("cur_team_match_results_hist_23_25")}` AS
    WITH base AS (
        SELECT *
        FROM `{table_id("cur_matches_hist_23_25")}`
        -- sem filtro de status aqui (ALL)
    )

    SELECT
        match_id,
        season,
        matchday,
        match_date,
        status,

        home_team_id AS team_id,
        away_team_id AS opponent_team_id,
        TRUE AS is_home,

        goals_home AS goals_for,
        goals_away AS goals_against,

        CASE WHEN status='FINISHED' THEN (goals_home - goals_away) ELSE NULL END AS goal_diff,

        CASE
            WHEN status!='FINISHED' THEN NULL
            WHEN goals_home > goals_away THEN 3
            WHEN goals_home = goals_away THEN 1
            ELSE 0
        END AS points,

        CASE
            WHEN status!='FINISHED' THEN NULL
            WHEN goals_home > goals_away THEN 'W'
            WHEN goals_home = goals_away THEN 'D'
            ELSE 'L'
        END AS result
    FROM base

    UNION ALL

    SELECT
        match_id,
        season,
        matchday,
        match_date,
        status,

        away_team_id AS team_id,
        home_team_id AS opponent_team_id,
        FALSE AS is_home,

        goals_away AS goals_for,
        goals_home AS goals_against,

        CASE WHEN status='FINISHED' THEN (goals_away - goals_home) ELSE NULL END AS goal_diff,

        CASE
            WHEN status!='FINISHED' THEN NULL
            WHEN goals_away > goals_home THEN 3
            WHEN goals_away = goals_home THEN 1
            ELSE 0
        END AS points,

        CASE
            WHEN status!='FINISHED' THEN NULL
            WHEN goals_away > goals_home THEN 'W'
            WHEN goals_away = goals_home THEN 'D'
            ELSE 'L'
        END AS result
    FROM base
    """
    run_query(client, query, "cur_team_match_results_hist_23_25")


def create_dim_teams_latest(client: bigquery.Client) -> None:
    query = f"""
    CREATE OR REPLACE TABLE `{table_id("dim_teams_latest")}` AS
    WITH latest AS (
      SELECT
        team_id,
        team_name,
        team_short_name,
        team_abbr,
        season AS season_source
      FROM `{table_id("raw_teams")}`
      QUALIFY ROW_NUMBER() OVER (PARTITION BY team_id ORDER BY season DESC) = 1
    )
    SELECT
      l.team_id,
      l.team_name,
      COALESCE(o.team_short_name, l.team_short_name) AS team_short_name,
      COALESCE(o.team_abbr, l.team_abbr) AS team_abbr,
      l.season_source
    FROM latest l
    LEFT JOIN `{table_id("cur_team_overrides")}` o
      ON o.team_id = l.team_id
    """
    run_query(client, query, "dim_teams_latest")


def create_agg_team_season_2026(client: bigquery.Client) -> None:
    query = f"""
    CREATE OR REPLACE TABLE `{table_id("agg_team_season_2026")}` AS
    WITH base AS (
        SELECT
            season,
            team_id,
            points,
            result,
            goals_for,
            goals_against,
            goal_diff
        FROM `{table_id("cur_team_match_results_2026")}`
    )
    SELECT
        b.season,
        b.team_id,
        t.team_name,
        t.team_short_name,
        t.team_abbr,

        COUNT(1) AS games,
        COUNTIF(b.points IS NOT NULL) AS games_played,

        SUM(COALESCE(b.points, 0)) AS points,
        SUM(CASE WHEN b.points IS NOT NULL AND b.result = 'W' THEN 1 ELSE 0 END) AS wins,
        SUM(CASE WHEN b.points IS NOT NULL AND b.result = 'D' THEN 1 ELSE 0 END) AS draws,
        SUM(CASE WHEN b.points IS NOT NULL AND b.result = 'L' THEN 1 ELSE 0 END) AS losses,

        SUM(COALESCE(b.goals_for, 0)) AS goals_for,
        SUM(COALESCE(b.goals_against, 0)) AS goals_against,
        SUM(COALESCE(b.goal_diff, 0)) AS goal_diff,

        SAFE_DIVIDE(
            SUM(COALESCE(b.points, 0)),
            NULLIF(COUNTIF(b.points IS NOT NULL), 0)
        ) AS ppj
    FROM base b
    LEFT JOIN `{table_id("dim_teams_latest")}` t
        ON t.team_id = b.team_id
    GROUP BY 1,2,3,4,5;
    """
    run_query(client, query, "agg_team_season_2026")


def create_agg_team_season_hist_23_25(client: bigquery.Client) -> None:
    query = f"""
    CREATE OR REPLACE TABLE `{table_id("agg_team_season_hist_23_25")}` AS
    WITH base AS (
      SELECT
        season,
        team_id,
        is_home,
        points,
        result,
        goals_for,
        goals_against,
        goal_diff
      FROM `{table_id("cur_team_match_results_hist_23_25")}`
    )
    SELECT
      b.season,
      b.team_id,
      t.team_name,
      t.team_short_name,
      t.team_abbr,

      CASE WHEN b.is_home THEN 'Casa' ELSE 'Fora' END AS home_away,

      COUNT(1) AS games,
      COUNTIF(b.points IS NOT NULL) AS games_played,

      SUM(COALESCE(b.points, 0)) AS points,
      SUM(CASE WHEN b.points IS NOT NULL AND b.result = 'W' THEN 1 ELSE 0 END) AS wins,
      SUM(CASE WHEN b.points IS NOT NULL AND b.result = 'D' THEN 1 ELSE 0 END) AS draws,
      SUM(CASE WHEN b.points IS NOT NULL AND b.result = 'L' THEN 1 ELSE 0 END) AS losses,

      SUM(COALESCE(b.goals_for, 0)) AS goals_for,
      SUM(COALESCE(b.goals_against, 0)) AS goals_against,
      SUM(COALESCE(b.goal_diff, 0)) AS goal_diff,

      SAFE_DIVIDE(SUM(COALESCE(b.points, 0)), NULLIF(COUNTIF(b.points IS NOT NULL), 0)) AS ppj
    FROM base b
    LEFT JOIN `{table_id("dim_teams_latest")}` t
      ON t.team_id = b.team_id
    GROUP BY 1,2,3,4,5,6;
    """
    run_query(client, query, "agg_team_season_hist_23_25")


def run_curated_layer(client):
    print("\n=== Criando camada CURATED ===\n")

    create_cur_matches_2026(client)
    create_cur_matches_hist(client)

    create_cur_team_results_2026(client)
    create_cur_team_results_hist(client)

    # overrides precisam existir ANTES da dim canônica
    create_cur_team_overrides(client)
    create_dim_teams_latest(client)

    # aggs de temporada
    create_agg_team_season_2026(client)
    create_agg_team_season_hist_23_25(client)

    print("\n✅ Camada curated criada com sucesso\n")