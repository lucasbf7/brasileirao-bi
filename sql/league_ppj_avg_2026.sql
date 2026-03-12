CREATE OR REPLACE VIEW `{project_id}.{dataset}.vw_league_ppj_avg_2026` AS
SELECT
  2026 AS season,
  AVG(ppj_acumulado_overall) AS league_ppj_avg,
  AVG(ppj_last5_overall) AS league_ppj_last5_avg
FROM `{project_id}.{dataset}.agg_form_2026`;