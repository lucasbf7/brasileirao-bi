CREATE OR REPLACE VIEW `{project_id}.{dataset}.vw_over_under_achievers_2026` AS
WITH proj AS (
  SELECT *
  FROM `{project_id}.{dataset}.vw_league_projection`
  WHERE season = 2026
),

trend AS (
  SELECT *
  FROM `{project_id}.{dataset}.vw_team_trend_2026`
  WHERE season = 2026
),

avg AS (
  SELECT *
  FROM `{project_id}.{dataset}.vw_league_ppj_avg_2026`
  WHERE season = 2026
),

dim AS (
  SELECT
    team_id,
    team_name,
    team_short_name,
    team_abbr,
    badge_url
  FROM `{project_id}.{dataset}.vw_dim_teams`
)

SELECT
  p.season,
  p.team_id,

  d.team_name,
  d.team_short_name,
  d.team_abbr,
  d.badge_url,

  p.pontos_atuais,
  p.pontos_projetados,
  p.jogos_restantes,
  p.ppj_estimado,
  p.destino,
  p.posicao_atual,
  p.posicao_projetada,

  t.ppj_geral,
  t.ppj_last5,
  t.delta_ppj_forma,
  t.tendencia_label,

  a.league_ppj_avg,
  a.league_ppj_last5_avg,

  (t.ppj_geral - a.league_ppj_avg) AS delta_ppj_vs_liga,
  (t.ppj_last5 - a.league_ppj_last5_avg) AS delta_ppj_last5_vs_liga,

  CASE
    WHEN (t.ppj_last5 - a.league_ppj_last5_avg) >= 0.20 THEN 'Acima da média'
    WHEN (t.ppj_last5 - a.league_ppj_last5_avg) <= -0.20 THEN 'Abaixo da média'
    ELSE 'Na média'
  END AS over_under_class
FROM proj p
LEFT JOIN dim d
  ON d.team_id = p.team_id
LEFT JOIN trend t
  ON t.team_id = p.team_id
CROSS JOIN avg a;