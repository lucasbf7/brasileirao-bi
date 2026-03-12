CREATE OR REPLACE VIEW `{project_id}.{dataset}.vw_future_matches_next5_2026` AS
SELECT *
FROM (
  SELECT
    season,
    matchday,
    match_id,
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
    IF(is_home, 'Casa', 'Fora') AS local_jogo,

    categoria_matchup,

    expected_result_bucket_coerente AS resultado_esperado,

    expected_points,
    expected_points_adj,

    ppj_final_previsao,
    origem_ppj,

    ROW_NUMBER() OVER (
      PARTITION BY team_id
      ORDER BY
        DATE(match_date) ASC NULLS LAST,
        matchday ASC,
        match_id ASC
    ) AS rn
  FROM `{project_id}.{dataset}.vw_projection_by_match_2026`
  WHERE status NOT IN ("FINISHED")
    AND NOT (
      status = "POSTPONED"
      AND (
        match_date IS NULL
        OR DATE(match_date) < CURRENT_DATE()
      )
    )
)
WHERE rn <= 5