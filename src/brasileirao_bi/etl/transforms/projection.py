from __future__ import annotations

from brasileirao_bi.bq.client import table_id
from brasileirao_bi.etl.transforms.mart import run_query


def create_fact_projection_matches(client, alpha_difficulty: float = 0.35) -> None:
    """
    Projeção por partida futura (uma linha por time por jogo futuro),
    partindo de dim_future_matches_enriched.

    Observação: aqui a granularidade já é "team-level" (team_id + opponent_team_id),
    então o output serve tanto pra tabela de próximos jogos quanto pra agregações.
    """
    query = f"""
    CREATE OR REPLACE TABLE `{table_id("fact_projection_matches")}` AS
    WITH base AS (
        SELECT
            d.*,
            (1 - d.score_dificuldade_matchup) AS score_facilidade_matchup
        FROM `{table_id("dim_future_matches_enriched")}` d
    )
    SELECT
        match_id,
        season,
        matchday,
        match_date,
        status,

        -- granularidade correta (por time)
        team_id,
        team_name,
        team_short_name,
        team_abbr,

        is_home,
        turno_returno,
        ord_turno_returno,

        opponent_team_id,
        opp_team_name,
        opp_team_short_name,
        opp_team_abbr,

        hist_jogos_adv_local_23_25,
        ppj_hist_adv_local_23_25,
        saldo_gols_hist_adv_local_23_25,
        ppj_hist_fallback_local_23_25,
        ppj_forma_2026,
        ppj_hist_base,
        ppj_final_previsao,
        origem_ppj,

        ppj_vs_opp_shrink,
        score_dificuldade_matchup,
        categoria_matchup,
        score_facilidade_matchup,

        -- expected_points base
        ppj_final_previsao AS expected_points,

        -- expected_points ajustado por dificuldade
        GREATEST(
            0.0,
            LEAST(
                3.0,
                ppj_final_previsao * (
                    1 + ((0.5 - COALESCE(score_facilidade_matchup, 0.5)) * {alpha_difficulty})
                )
            )
        ) AS expected_points_adj,

        -- bucket base
        CASE
            WHEN ppj_final_previsao >= 2.5 THEN 3
            WHEN ppj_final_previsao >= 1.0 THEN 1
            ELSE 0
        END AS expected_points_bucket,

        -- bucket ajustado
        CASE
            WHEN (
                GREATEST(
                    0.0,
                    LEAST(
                        3.0,
                        ppj_final_previsao * (
                            1 + ((0.5 - COALESCE(score_facilidade_matchup, 0.5)) * {alpha_difficulty})
                        )
                    )
                )
            ) >= 2.5 THEN 3
            WHEN (
                GREATEST(
                    0.0,
                    LEAST(
                        3.0,
                        ppj_final_previsao * (
                            1 + ((0.5 - COALESCE(score_facilidade_matchup, 0.5)) * {alpha_difficulty})
                        )
                    )
                )
            ) >= 1.0 THEN 1
            ELSE 0
        END AS expected_points_bucket_adj,

        CASE
            WHEN ppj_final_previsao >= 2.5 THEN 'Vitória'
            WHEN ppj_final_previsao >= 1.0 THEN 'Empate'
            ELSE 'Derrota'
        END AS expected_result_bucket
    FROM base;
    """
    run_query(client, query, "fact_projection_matches")


def create_fact_projection_micro(client) -> None:
    """
    Projeção "micro" por (season, team_id):
      - estado atual (último jogo finalizado do time na temporada)
      - soma das expectativas dos jogos restantes
      - destino (classificação/objetivo) calculado em SQL
    """
    query = f"""
    CREATE OR REPLACE TABLE `{table_id("fact_projection_micro")}` AS
    WITH last_state AS (
        SELECT *
        FROM `{table_id("fact_matches_running")}`
        WHERE is_played = 1
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY season, team_id
            ORDER BY matchday DESC, match_date DESC, match_id DESC
        ) = 1
    ),
    future AS (
        SELECT
            season,
            team_id,

            COUNT(1) AS games_remaining,

            SUM(expected_points) AS expected_points_remaining,
            SUM(expected_points_adj) AS expected_points_adj_remaining,

            SUM(expected_points_bucket) AS expected_points_bucket_remaining,
            SUM(expected_points_bucket_adj) AS expected_points_bucket_adj_remaining
        FROM `{table_id("fact_projection_matches")}`
        GROUP BY 1,2
    ),
    base AS (
        SELECT
            ls.season,
            ls.team_id,
            ls.team_name,
            ls.team_short_name,
            ls.team_abbr,

            ls.matchday AS matchday_atual,
            ls.pontos_acumulados AS points_so_far,
            ls.jogos_disputados AS games_played,

            COALESCE(f.games_remaining, 0) AS games_remaining,

            -- contínuo (base)
            COALESCE(f.expected_points_remaining, 0.0) AS expected_points_remaining,
            (ls.pontos_acumulados + COALESCE(f.expected_points_remaining, 0.0)) AS projected_points_total,
            SAFE_DIVIDE((ls.pontos_acumulados + COALESCE(f.expected_points_remaining, 0.0)), 38) AS projected_ppj_total,

            -- contínuo (ajustado)
            COALESCE(f.expected_points_adj_remaining, 0.0) AS expected_points_adj_remaining,
            (ls.pontos_acumulados + COALESCE(f.expected_points_adj_remaining, 0.0)) AS projected_points_total_adj,
            SAFE_DIVIDE((ls.pontos_acumulados + COALESCE(f.expected_points_adj_remaining, 0.0)), 38) AS projected_ppj_total_adj,

            -- discreto (base)
            COALESCE(f.expected_points_bucket_remaining, 0) AS expected_points_bucket_remaining,
            (ls.pontos_acumulados + COALESCE(f.expected_points_bucket_remaining, 0)) AS projected_points_total_bucket,
            SAFE_DIVIDE((ls.pontos_acumulados + COALESCE(f.expected_points_bucket_remaining, 0)), 38) AS projected_ppj_total_bucket,

            -- discreto (ajustado)
            COALESCE(f.expected_points_bucket_adj_remaining, 0) AS expected_points_bucket_adj_remaining,
            (ls.pontos_acumulados + COALESCE(f.expected_points_bucket_adj_remaining, 0)) AS projected_points_total_bucket_adj,
            SAFE_DIVIDE((ls.pontos_acumulados + COALESCE(f.expected_points_bucket_adj_remaining, 0)), 38) AS projected_ppj_total_bucket_adj,

            -- comparação base
            (
              (ls.pontos_acumulados + COALESCE(f.expected_points_remaining, 0.0))
              - (ls.pontos_acumulados + COALESCE(f.expected_points_bucket_remaining, 0))
            ) AS delta_points_continuous_vs_bucket,

            (
              SAFE_DIVIDE((ls.pontos_acumulados + COALESCE(f.expected_points_remaining, 0.0)), 38)
              - SAFE_DIVIDE((ls.pontos_acumulados + COALESCE(f.expected_points_bucket_remaining, 0)), 38)
            ) AS delta_ppj_continuous_vs_bucket,

            -- comparação ajustada
            (
              (ls.pontos_acumulados + COALESCE(f.expected_points_adj_remaining, 0.0))
              - (ls.pontos_acumulados + COALESCE(f.expected_points_bucket_adj_remaining, 0))
            ) AS delta_points_adj_vs_bucket_adj,

            (
              SAFE_DIVIDE((ls.pontos_acumulados + COALESCE(f.expected_points_adj_remaining, 0.0)), 38)
              - SAFE_DIVIDE((ls.pontos_acumulados + COALESCE(f.expected_points_bucket_adj_remaining, 0)), 38)
            ) AS delta_ppj_adj_vs_bucket_adj
        FROM last_state ls
        LEFT JOIN future f
          ON f.season = ls.season
         AND f.team_id = ls.team_id
    )
    SELECT
        b.*,

        CASE
            WHEN b.projected_ppj_total >= 2.05 THEN 'Briga por Título'
            WHEN b.projected_ppj_total >= 1.68 THEN 'Libertadores (G-4)'
            WHEN b.projected_ppj_total >= 1.53 THEN 'Pré-Libertadores (G-6)'
            WHEN b.projected_ppj_total >= 1.31 THEN 'Sul-Americana (G-12)'
            WHEN b.projected_ppj_total >= 1.18 THEN 'Permanece'
            WHEN b.projected_ppj_total >= 1.13 THEN 'Praticamente Rebaixado'
            ELSE 'Rebaixado'
        END AS destino
    FROM base b;
    """
    run_query(client, query, "fact_projection_micro")