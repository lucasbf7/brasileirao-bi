CREATE OR REPLACE VIEW `{project_id}.{dataset}.vw_team_trend_2026` AS
WITH f AS (
  SELECT
    2026 AS season,
    team_id,

    ppj_acumulado_overall AS ppj_geral,
    ppj_last5_overall AS ppj_last5,

    (ppj_last5_overall - ppj_acumulado_overall) AS delta_ppj_forma
  FROM `{project_id}.{dataset}.agg_form_2026`
)

SELECT
  season,
  team_id,
  ppj_geral,
  ppj_last5,
  delta_ppj_forma,

  CASE
    WHEN delta_ppj_forma >= 0.25 THEN 'Forte subida'
    WHEN delta_ppj_forma >= 0.10 THEN 'Subindo'
    WHEN delta_ppj_forma > -0.10 THEN 'Estável'
    WHEN delta_ppj_forma > -0.25 THEN 'Caindo'
    ELSE 'Forte queda'
  END AS tendencia_label
FROM f;