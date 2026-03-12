CREATE OR REPLACE VIEW `{project_id}.{dataset}.vw_points_real_vs_projected_2026` AS
WITH
spine AS (
  SELECT matchday
  FROM UNNEST(GENERATE_ARRAY(1, 38)) AS matchday
),

last_state AS (
  SELECT
    season,
    team_id,
    team_name,
    team_short_name,
    team_abbr,
    matchday AS matchday_atual,
    pontos_acumulados AS points_so_far
  FROM `{project_id}.{dataset}.fact_matches_running`
  WHERE season = 2026
    AND is_played = 1
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY season, team_id
    ORDER BY matchday DESC, match_date DESC, match_id DESC
  ) = 1
),

real_by_matchday AS (
  SELECT
    season,
    team_id,
    matchday,
    MAX(pontos_acumulados) AS points_real_cum
  FROM `{project_id}.{dataset}.fact_matches_running`
  WHERE season = 2026
    AND is_played = 1
  GROUP BY season, team_id, matchday
),

expected_by_matchday AS (
  SELECT
    season,
    team_id,
    matchday,
    SUM(expected_points) AS expected_points_matchday
  FROM `{project_id}.{dataset}.fact_projection_matches`
  WHERE season = 2026
    AND status NOT IN ("FINISHED")
    AND NOT (
      status = "POSTPONED"
      AND (
        match_date IS NULL
        OR DATE(match_date) < CURRENT_DATE()
      )
    )
  GROUP BY season, team_id, matchday
),

expected_cum AS (
  SELECT
    season,
    team_id,
    matchday,
    SUM(expected_points_matchday) OVER (
      PARTITION BY season, team_id
      ORDER BY matchday
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS expected_points_cum
  FROM expected_by_matchday
),

grid AS (
  SELECT
    ls.season,
    ls.team_id,
    ls.team_name,
    ls.team_short_name,
    ls.team_abbr,
    ls.matchday_atual,
    ls.points_so_far,
    s.matchday
  FROM last_state ls
  CROSS JOIN spine s
)

SELECT
  g.season,
  g.team_id,
  g.team_name,
  g.team_short_name,
  g.team_abbr,
  g.matchday,
  g.matchday_atual,

  -- REAL: só até a rodada atual (depois NULL)
  CASE
    WHEN g.matchday <= g.matchday_atual THEN r.points_real_cum
    ELSE NULL
  END AS points_real,

  -- PROJETADO: aparece só da rodada atual em diante
  CASE
    WHEN g.matchday < g.matchday_atual THEN NULL
    ELSE (
      g.points_so_far
      + COALESCE(ec.expected_points_cum, 0.0)
      - COALESCE(ec_atual.expected_points_cum, 0.0)
    )
  END AS points_projected,

  -- marcador "HOJE" (ponto destacado)
  CASE
    WHEN g.matchday = g.matchday_atual THEN r.points_real_cum
    ELSE NULL
  END AS points_real_hoje,

  CASE
    WHEN g.matchday = g.matchday_atual THEN 'Hoje'
    ELSE NULL
  END AS label_hoje,

  SAFE_DIVIDE(
    CASE WHEN g.matchday <= g.matchday_atual THEN r.points_real_cum ELSE NULL END,
    NULLIF(g.matchday, 0)
  ) AS ppj_real,

  SAFE_DIVIDE(
    CASE
      WHEN g.matchday < g.matchday_atual THEN NULL
      ELSE (
        g.points_so_far
        + COALESCE(ec.expected_points_cum, 0.0)
        - COALESCE(ec_atual.expected_points_cum, 0.0)
      )
    END,
    38
  ) AS ppj_projected

FROM grid g
LEFT JOIN real_by_matchday r
  ON r.season = g.season
 AND r.team_id = g.team_id
 AND r.matchday = g.matchday
LEFT JOIN expected_cum ec
  ON ec.season = g.season
 AND ec.team_id = g.team_id
 AND ec.matchday = g.matchday
LEFT JOIN expected_cum ec_atual
  ON ec_atual.season = g.season
 AND ec_atual.team_id = g.team_id
 AND ec_atual.matchday = g.matchday_atual