CREATE OR REPLACE VIEW `{project_id}.{dataset}.vw_projection_by_match_2026` AS
WITH base AS (
  SELECT
    f.season,
    f.matchday,
    f.match_id,
    f.match_date,
    f.status,

    f.team_id,
    t.team_name,
    t.team_short_name,
    t.team_abbr,

    f.opponent_team_id,
    o.team_name        AS opp_team_name,
    o.team_short_name  AS opp_team_short_name,
    o.team_abbr        AS opp_team_abbr,

    f.is_home,
    IF(f.is_home, 'Casa', 'Fora') AS local_jogo,

    f.categoria_matchup,

    f.expected_result_bucket AS expected_result_bucket_raw,

    f.expected_points,
    f.expected_points_adj,

    f.ppj_final_previsao,
    f.origem_ppj
  FROM `{project_id}.{dataset}.fact_projection_matches` f
  LEFT JOIN `{project_id}.{dataset}.vw_dim_teams` t
    ON t.team_id = f.team_id
  LEFT JOIN `{project_id}.{dataset}.vw_dim_teams` o
    ON o.team_id = f.opponent_team_id
  WHERE f.season = 2026
),

paired AS (
  SELECT
    a.*,
    b.expected_result_bucket_raw AS other_bucket
  FROM base a
  LEFT JOIN base b
    ON b.match_id = a.match_id
   AND b.team_id  = a.opponent_team_id
),

final AS (
  SELECT
    *,
    CASE
      -- mesma previsão pros 2 lados => empate pros 2 (Vit/Vit, Emp/Emp, Der/Der)
      WHEN expected_result_bucket_raw = other_bucket THEN 'Empate'

      -- um lado "Empate" e o outro é "Derrota" => Empate vira Vitória (e o outro mantém Derrota)
      WHEN expected_result_bucket_raw = 'Empate' AND other_bucket = 'Derrota' THEN 'Vitória'

      -- um lado "Empate" e o outro é "Vitória" => Empate vira Derrota (e o outro mantém Vitória)
      WHEN expected_result_bucket_raw = 'Empate' AND other_bucket = 'Vitória' THEN 'Derrota'

      -- casos Vitória/Derrota (ou Derrota/Vitória) ficam como estão
      ELSE expected_result_bucket_raw
    END AS expected_result_bucket_coerente
  FROM paired
)

SELECT * FROM final;