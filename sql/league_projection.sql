CREATE OR REPLACE VIEW `{project_id}.{dataset}.vw_league_projection` AS
WITH base AS (
  SELECT
    season,
    team_id,
    team_name,
    team_short_name,
    team_abbr,

    points_so_far AS pontos_atuais,
    projected_points_total AS pontos_projetados,
    games_remaining AS jogos_restantes,
    projected_ppj_total AS ppj_estimado,
    destino,

    CURRENT_TIMESTAMP() AS updated_at
  FROM `{project_id}.{dataset}.fact_projection_micro`
  WHERE season = 2026
),
season_stats AS (
  SELECT
    team_id,
    ANY_VALUE(wins) AS vitorias,
    ANY_VALUE(goal_diff) AS saldo_gols,
    ANY_VALUE(goals_for) AS gols_pro
  FROM `{project_id}.{dataset}.agg_team_season_2026`
  GROUP BY team_id
),
ranked AS (
  SELECT
    b.*,
    s.vitorias,
    s.saldo_gols,
    s.gols_pro,

    RANK() OVER (
      PARTITION BY b.season
      ORDER BY
        b.pontos_atuais DESC,
        s.vitorias DESC,
        s.saldo_gols DESC,
        s.gols_pro DESC,
        b.team_id
    ) AS posicao_atual,

    RANK() OVER (
      PARTITION BY b.season
      ORDER BY
        b.pontos_projetados DESC,
        b.team_id
    ) AS posicao_projetada
  FROM base b
  LEFT JOIN season_stats s USING (team_id)
)
SELECT
  *,
  (posicao_projetada - posicao_atual) AS delta_posicao
FROM ranked;