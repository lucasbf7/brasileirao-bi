CREATE OR REPLACE VIEW `{project_id}.{dataset}.vw_team_trend_matchday` AS
SELECT
  season,
  team_id,
  team_name,
  team_short_name,
  team_abbr,

  matchday,
  match_date,
  utc_date,
  status,

  is_home,
  turno_returno,
  ord_turno_returno,

  points,
  pontos_acumulados,
  jogos_disputados,
  ppj_acumulado,
  ppj_ultimos_5,

  goals_for,
  goals_against,
  goal_diff,
  saldo_gols_acumulado,

  result,
  is_played,
  clean_sheet,

  opponent_team_id,
  opp_team_name,
  opp_team_short_name,
  opp_team_abbr
FROM `{project_id}.{dataset}.fact_matches_running`
WHERE season = 2026