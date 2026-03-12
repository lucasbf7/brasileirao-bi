CREATE OR REPLACE VIEW `{project_id}.{dataset}.vw_monte_carlo_matches_2026` AS
WITH base AS (
  SELECT
    season,
    match_id,
    matchday,
    match_date,
    status,

    team_id,
    team_name,
    team_short_name,
    team_abbr,

    opponent_team_id,
    opp_team_name,
    opp_team_short_name,
    opp_team_abbr,

    is_home,
    categoria_matchup,
    expected_result_bucket_coerente,
    expected_points,
    ppj_final_previsao
  FROM `{project_id}.{dataset}.vw_projection_by_match_2026`
  WHERE season = 2026
),

home_side AS (
  SELECT
    season,
    match_id,
    matchday,
    match_date,
    status,

    team_id AS home_team_id,
    team_name AS home_team_name,
    team_short_name AS home_team_short_name,
    team_abbr AS home_team_abbr,

    opponent_team_id AS away_team_id,
    opp_team_name AS away_team_name,
    opp_team_short_name AS away_team_short_name,
    opp_team_abbr AS away_team_abbr,

    categoria_matchup AS home_categoria_matchup,
    expected_result_bucket_coerente AS home_expected_result,
    expected_points AS home_expected_points,
    ppj_final_previsao AS home_ppj_previsao
  FROM base
  WHERE is_home = TRUE
),

away_side AS (
  SELECT
    match_id,
    categoria_matchup AS away_categoria_matchup,
    expected_result_bucket_coerente AS away_expected_result,
    expected_points AS away_expected_points,
    ppj_final_previsao AS away_ppj_previsao
  FROM base
  WHERE is_home = FALSE
)

SELECT
  h.season,
  h.match_id,
  h.matchday,
  h.match_date,
  h.status,

  h.home_team_id,
  h.home_team_name,
  h.home_team_short_name,
  h.home_team_abbr,

  h.away_team_id,
  h.away_team_name,
  h.away_team_short_name,
  h.away_team_abbr,

  h.home_categoria_matchup,
  a.away_categoria_matchup,

  h.home_expected_result,
  a.away_expected_result,

  h.home_expected_points,
  a.away_expected_points,

  h.home_ppj_previsao,
  a.away_ppj_previsao

FROM home_side h
LEFT JOIN away_side a
  ON a.match_id = h.match_id;