CREATE OR REPLACE VIEW `{project_id}.{dataset}.vw_monte_carlo_summary_2026` AS
SELECT
  mc.season,
  mc.team_id,
  mc.team_name,
  mc.team_short_name,
  mc.team_abbr,

  a.points AS pontos_atuais,
  a.wins AS vitorias_atuais,
  a.goal_diff AS saldo_gols_atual,
  a.goals_for AS gols_pro_atual,

  lp.posicao_atual,
  lp.posicao_projetada,
  lp.pontos_projetados,
  lp.ppj_estimado,

  mc.avg_final_points,
  mc.avg_final_position,
  mc.prob_titulo,
  mc.prob_g4,
  mc.prob_g6,
  mc.prob_sulamericana,
  mc.prob_rebaixamento,
  mc.n_sims

FROM `{project_id}.{dataset}.monte_carlo_results_2026` mc
LEFT JOIN `{project_id}.{dataset}.agg_team_season_2026` a
  ON a.team_id = mc.team_id
LEFT JOIN `{project_id}.{dataset}.vw_league_projection` lp
  ON lp.team_id = mc.team_id
 AND lp.season = mc.season;