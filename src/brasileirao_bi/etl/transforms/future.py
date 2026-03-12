from __future__ import annotations

from brasileirao_bi.bq.client import table_id
from brasileirao_bi.etl.transforms.mart import run_query


def create_dim_future_matches_enriched(
    client,
    min_hist_games: int = 3,
    w_hist_fallback: float = 0.2,   # histórico fraco
    w_form_fallback: float = 0.8,   # forma manda no fallback
) -> None:
    query = f"""
    CREATE OR REPLACE TABLE `{table_id("dim_future_matches_enriched")}` AS
    WITH
    future AS (
        SELECT *
        FROM `{table_id("fact_matches_running")}`
        WHERE is_played = 0
    ),

    adv_local AS (
        SELECT
            team_id, opponent_team_id, is_home,
            jogos,
            ppj_hist_adv_local,
            saldo_gols_hist_adv_local
        FROM `{table_id("agg_hist_adv_local_23_25")}`
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY team_id, opponent_team_id, is_home
            ORDER BY jogos DESC
        ) = 1
    ),

    fallback_local AS (
        SELECT
            team_id, is_home,
            ppj_hist_fallback_local
        FROM `{table_id(".agg_hist_local_23_25")}`
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY team_id, is_home
            ORDER BY ppj_hist_fallback_local DESC
        ) = 1
    ),

    form_2026 AS (
        SELECT
            team_id, is_home,
            ppj_last5_local,
            ppj_last5_overall,
            ppj_acumulado_overall,
            pontos_acumulados,
            jogos_disputados
        FROM `{table_id(".agg_form_2026")}`
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY team_id, is_home
            ORDER BY jogos_disputados DESC, pontos_acumulados DESC
        ) = 1
    ),

    opp_score AS (
        SELECT
            team_id, opponent_team_id, is_home,
            ppj_shrink,
            score_dificuldade,
            categoria
        FROM `{table_id(".agg_opponent_score_23_25")}`
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY team_id, opponent_team_id, is_home
            ORDER BY score_dificuldade DESC
        ) = 1
    ),

    base AS (
        SELECT
            f.match_id,
            f.season,
            f.matchday,
            f.match_date,
            f.utc_date,
            f.status,

            f.team_id,
            f.opponent_team_id,
            f.is_home,

            f.turno_returno,
            f.ord_turno_returno,

            f.team_name,
            f.team_short_name,
            f.team_abbr,

            f.opp_team_name,
            f.opp_team_short_name,
            f.opp_team_abbr,

            al.jogos AS hist_jogos_adv_local_23_25,
            al.ppj_hist_adv_local AS ppj_hist_adv_local_23_25,
            al.saldo_gols_hist_adv_local AS saldo_gols_hist_adv_local_23_25,

            fl.ppj_hist_fallback_local AS ppj_hist_fallback_local_23_25,

            COALESCE(
                f26.ppj_last5_local,
                f26.ppj_last5_overall,
                f26.ppj_acumulado_overall
            ) AS ppj_forma_2026,

            f26.jogos_disputados AS jogos_disputados_2026,
            f26.pontos_acumulados AS pontos_acumulados_2026,

            os.ppj_shrink AS ppj_vs_opp_shrink,
            os.score_dificuldade AS score_dificuldade_matchup,
            COALESCE(os.categoria, 'N/A') AS categoria_matchup
        FROM future f
        LEFT JOIN adv_local al
            ON al.team_id = f.team_id
            AND al.opponent_team_id = f.opponent_team_id
            AND al.is_home = f.is_home
        LEFT JOIN fallback_local fl
            ON fl.team_id = f.team_id
            AND fl.is_home = f.is_home
        LEFT JOIN form_2026 f26
            ON f26.team_id = f.team_id
            AND f26.is_home = f.is_home
        LEFT JOIN opp_score os
            ON os.team_id = f.team_id
            AND os.opponent_team_id = f.opponent_team_id
            AND os.is_home = f.is_home
    ),

    scored AS (
        SELECT
            b.*,

            COALESCE(b.ppj_hist_adv_local_23_25, b.ppj_hist_fallback_local_23_25) AS ppj_hist_base,

            -- pesos dinâmicos (mantém comportamento)
            CASE
                WHEN COALESCE(b.jogos_disputados_2026, 0) >= 10 THEN 0.60
                WHEN COALESCE(b.jogos_disputados_2026, 0) >= 5  THEN 0.75
                ELSE 0.90
            END AS w_form_dyn,

            CASE
                WHEN COALESCE(b.jogos_disputados_2026, 0) >= 10 THEN 0.40
                WHEN COALESCE(b.jogos_disputados_2026, 0) >= 5  THEN 0.25
                ELSE 0.10
            END AS w_hist_dyn,

            -- peso do histórico adv_local baseado na amostra (0..1)
            LEAST(
                1.0,
                SAFE_DIVIDE(COALESCE(b.hist_jogos_adv_local_23_25, 0), CAST({min_hist_games} AS FLOAT64))
            ) AS w_adv_sample,

            -- histórico efetivo: se tenho adv_local, ele “puxa” o fallback conforme a amostra cresce
            (
                LEAST(
                1.0,
                SAFE_DIVIDE(COALESCE(b.hist_jogos_adv_local_23_25, 0), CAST({min_hist_games} AS FLOAT64))
                ) * COALESCE(b.ppj_hist_adv_local_23_25, b.ppj_hist_fallback_local_23_25)
                +
                (1.0 - LEAST(
                1.0,
                SAFE_DIVIDE(COALESCE(b.hist_jogos_adv_local_23_25, 0), CAST({min_hist_games} AS FLOAT64))
                )) * COALESCE(b.ppj_hist_fallback_local_23_25, b.ppj_hist_adv_local_23_25)
            ) AS ppj_hist_effective,

            -- PPJ final com 4 cenários
            CASE
                -- A) tem adv_local (mesmo que fraco): usa hist_effective + forma
                WHEN b.ppj_hist_adv_local_23_25 IS NOT NULL THEN
                ( (CASE
                        WHEN COALESCE(b.jogos_disputados_2026, 0) >= 10 THEN 0.40
                        WHEN COALESCE(b.jogos_disputados_2026, 0) >= 5  THEN 0.25
                        ELSE 0.10
                    END) * (
                    (
                        LEAST(
                        1.0,
                        SAFE_DIVIDE(COALESCE(b.hist_jogos_adv_local_23_25, 0), CAST({min_hist_games} AS FLOAT64))
                        ) * COALESCE(b.ppj_hist_adv_local_23_25, b.ppj_hist_fallback_local_23_25)
                        +
                        (1.0 - LEAST(
                        1.0,
                        SAFE_DIVIDE(COALESCE(b.hist_jogos_adv_local_23_25, 0), CAST({min_hist_games} AS FLOAT64))
                        )) * COALESCE(b.ppj_hist_fallback_local_23_25, b.ppj_hist_adv_local_23_25)
                    )
                    )
                )
                +
                ( (CASE
                        WHEN COALESCE(b.jogos_disputados_2026, 0) >= 10 THEN 0.60
                        WHEN COALESCE(b.jogos_disputados_2026, 0) >= 5  THEN 0.75
                        ELSE 0.90
                    END) * COALESCE(b.ppj_forma_2026, b.ppj_hist_adv_local_23_25, b.ppj_hist_fallback_local_23_25)
                )

                -- B) sem adv_local, mas tem fallback: fallback fraco + forma forte
                WHEN b.ppj_hist_fallback_local_23_25 IS NOT NULL THEN
                ({w_hist_fallback} * b.ppj_hist_fallback_local_23_25)
                +
                ({w_form_fallback} * COALESCE(b.ppj_forma_2026, b.ppj_hist_fallback_local_23_25))

                -- C) só forma
                WHEN b.ppj_forma_2026 IS NOT NULL THEN
                b.ppj_forma_2026

                -- D) nada
                ELSE 1.5
            END AS ppj_final_previsao,

            CASE
                WHEN b.ppj_hist_adv_local_23_25 IS NOT NULL THEN 'HIST_ADV_LOCAL_(SHRUNK)+FORM_DYN'
                WHEN b.ppj_hist_fallback_local_23_25 IS NOT NULL THEN 'HIST_FALLBACK_WEAK+FORM'
                WHEN b.ppj_forma_2026 IS NOT NULL THEN 'FORM_ONLY_NO_HIST'
                ELSE 'BASELINE_NO_DATA'
            END AS origem_ppj

            FROM base b
    )

    SELECT
        s.*,
        CASE
            WHEN s.ppj_final_previsao >=
                CASE
                    WHEN categoria_matchup = 'Freguês' THEN 1.8
                    WHEN categoria_matchup = 'Neutro' THEN 2.0
                    WHEN categoria_matchup = 'Carrasco' THEN 2.2
                    ELSE 2.0
                END            
            THEN 'Vitória'

            WHEN s.ppj_final_previsao >= 1.2 THEN 'Empate'
            ELSE 'Derrota'
        END AS resultado_esperado_matchup
    FROM scored s;
    """
    run_query(client, query, "dim_future_matches_enriched")