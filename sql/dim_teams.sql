CREATE OR REPLACE VIEW `{project_id}.{dataset}.vw_dim_teams` AS
SELECT DISTINCT
  team_id,
  team_name,
  team_short_name,
  team_abbr,

  LOWER(team_abbr) AS team_abbr_lower,

  CONCAT(
    'https://raw.githubusercontent.com/lucasbf7/vasco-bi/main/assets/badges/',
    UPPER(team_abbr),
    '.png'
  ) AS badge_url

FROM `{project_id}.{dataset}.fact_matches_running`