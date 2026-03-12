CREATE OR REPLACE VIEW `{project_id}.{dataset}.vw_team_form` AS
WITH last_state AS (
  SELECT
    season,
    team_id,
    team_name,
    team_short_name,
    team_abbr,
    pontos_acumulados,
    jogos_disputados
  FROM `{project_id}.{dataset}.fact_matches_running`
  WHERE season = 2026
    AND is_played = 1
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY season, team_id
    ORDER BY matchday DESC, match_date DESC, match_id DESC
  ) = 1
)
SELECT
  2026 AS season,
  ls.team_id,
  ls.team_name,
  ls.team_short_name,
  ls.team_abbr,

  f.ppj_last5_overall,

  ls.pontos_acumulados,
  ls.jogos_disputados,
  SAFE_DIVIDE(ls.pontos_acumulados, NULLIF(ls.jogos_disputados, 0)) AS ppj_geral
FROM last_state ls
LEFT JOIN `{project_id}.{dataset}.agg_form_2026` f
  ON f.team_id = ls.team_id