CREATE OR REPLACE VIEW `{project_id}.{dataset}.vw_monte_carlo_neighbors_2026` AS
WITH base AS (
  SELECT
    season,
    team_id,
    team_name,
    team_short_name,
    team_abbr,
    posicao_atual,
    posicao_projetada,
    avg_final_position,
    avg_final_points,
    prob_titulo,
    prob_g4,
    prob_g6,
    prob_sulamericana,
    prob_rebaixamento
  FROM `{project_id}.{dataset}.vw_monte_carlo_summary_2026`
  WHERE season = 2026
)
SELECT
  a.season,

  a.team_id AS anchor_team_id,
  a.team_name AS anchor_team_name,
  a.team_short_name AS anchor_team_short_name,
  a.posicao_atual AS anchor_posicao_atual,

  b.team_id,
  b.team_name,
  b.team_short_name,
  b.team_abbr,
  b.posicao_atual,
  b.posicao_projetada,
  b.avg_final_position,
  b.avg_final_points,
  b.prob_titulo,
  b.prob_g4,
  b.prob_g6,
  b.prob_sulamericana,
  b.prob_rebaixamento

FROM base a
JOIN base b
  ON a.season = b.season
WHERE ABS(b.posicao_atual - a.posicao_atual) <= 2;