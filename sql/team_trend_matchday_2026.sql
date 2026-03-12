CREATE OR REPLACE VIEW `{project_id}.{dataset}.vw_team_trend_matchday_2026` AS
WITH base AS (
  SELECT
    m.season,
    m.matchday,
    m.team_id,
    d.team_name,
    d.team_short_name,
    d.team_abbr,
    d.badge_url,

    CAST(m.goals_for AS INT64) AS gols_pro_jogo,
    CAST(m.goals_against AS INT64) AS gols_contra_jogo,
    CAST(m.points AS INT64) AS pontos_jogo

  FROM `{project_id}.{dataset}.matches_enriched` m
  LEFT JOIN `{project_id}.{dataset}.vw_dim_teams` d
    ON d.team_id = m.team_id
  WHERE m.season = 2026
    AND m.status = 'FINISHED'
),

acumulado AS (
  SELECT
    season,
    matchday,
    team_id,
    team_name,
    team_short_name,
    team_abbr,
    badge_url,

    gols_pro_jogo,
    gols_contra_jogo,
    (gols_pro_jogo - gols_contra_jogo) AS saldo_jogo,
    pontos_jogo,

    SUM(gols_pro_jogo) OVER (
      PARTITION BY season, team_id
      ORDER BY matchday
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS gols_pro_acumulados,

    SUM(gols_contra_jogo) OVER (
      PARTITION BY season, team_id
      ORDER BY matchday
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS gols_contra_acumulados,

    SUM(pontos_jogo) OVER (
      PARTITION BY season, team_id
      ORDER BY matchday
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS pontos_acumulados,

    COUNT(*) OVER (
      PARTITION BY season, team_id
      ORDER BY matchday
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS jogos_disputados

  FROM base
)

SELECT
  season,
  matchday,
  team_id,
  team_name,
  team_short_name,
  team_abbr,
  badge_url,

  gols_pro_jogo,
  gols_contra_jogo,
  saldo_jogo,
  pontos_jogo,

  gols_pro_acumulados,
  gols_contra_acumulados,
  (gols_pro_acumulados - gols_contra_acumulados) AS saldo_gols_acumulado,
  pontos_acumulados,

  SAFE_DIVIDE(pontos_acumulados, jogos_disputados) AS ppj_acumulado

FROM acumulado