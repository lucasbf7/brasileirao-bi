# Data Model

## Visão geral

O projeto usa uma modelagem em camadas, com tabelas de ingestão, tratamento, agregação, projeção e simulação.

---

## raw_matches

Grão: 1 linha por partida vinda da API.

Contém:
- metadados da partida
- temporada
- times mandante e visitante
- placar
- status

Uso:
base bruta de partidas

---

## raw_teams

Grão: 1 linha por time retornado pela API em cada temporada.

Contém:
- identificadores
- nomes
- siglas
- metadados do time

Uso:
base bruta de clubes

---

## cur_team_match_results_2026

Grão: 1 linha por time por partida na temporada.

Contém:
- pontos
- resultado
- gols pró
- gols contra
- saldo
- mando de campo
- adversário

Uso:
base consolidada por time para a temporada atual

---

## agg_team_season_2026

Grão: 1 linha por time na temporada.

Contém:
- pontos
- vitórias
- empates
- derrotas
- gols marcados
- gols sofridos
- saldo
- PPJ

Uso:
tabela-base de ranking atual

---

## agg_team_season_hist_23_25

Grão: 1 linha por time por temporada histórica.

Contém:
- desempenho agregado das temporadas 2023–2025

Uso:
apoio histórico para projeção

---

## matches_enriched

Grão: 1 linha por partida enriquecida.

Contém:
- atributos adicionais de contexto
- chaves de relacionamento
- informações tratadas para consumo analítico

Uso:
base enriquecida de partidas

---

## fact_matches_running

Grão: 1 linha por time por partida, em ordem cronológica.

Contém:
- pontos acumulados
- jogos disputados
- PPJ acumulado
- PPJ últimos 5 jogos
- saldo acumulado
- adversário
- mando
- data

Uso:
base principal para análise temporal e forma

---

## agg_form_2026

Grão: 1 linha por time na temporada atual.

Contém:
- PPJ acumulado
- PPJ últimos 5
- indicadores de forma

Uso:
medir momento recente do time

---

## agg_hist_adv_local_23_25

Grão: adversário + casa/fora.

Contém:
- PPJ histórico
- saldo histórico
- estatísticas resumidas por contexto

Uso:
base histórica por confronto/contexto

---

## agg_opponent_score_23_25

Grão: adversário + casa/fora.

Contém:
- score de dificuldade
- PPJ ajustado
- categoria do confronto

Uso:
ajuste de dificuldade para jogos futuros

---

## dim_future_matches_enriched

Grão: 1 linha por jogo futuro.

Contém:
- features finais para projeção
- PPJ base
- PPJ ajustado
- categoria do confronto
- expectativa de pontos

Uso:
input da camada de projeção

---

## fact_projection_matches

Grão: 1 linha por jogo futuro.

Contém:
- expected_points
- expected_points_adj
- bucket de resultado esperado
- PPJ final de previsão

Uso:
projeção jogo a jogo

---

## fact_projection_micro

Grão: 1 linha por time por temporada.

Contém:
- pontos atuais
- pontos projetados
- jogos restantes
- PPJ projetado
- destino/cenário

Uso:
base principal da tabela projetada

---

## monte_carlo_results_2026

Grão: 1 linha por time.

Contém:
- média de pontos finais
- média de posição final
- probabilidades de faixas de classificação
- posição mais provável

Uso:
resumo probabilístico por time

---

## monte_carlo_position_distribution_2026

Grão: 1 linha por time por posição final.

Contém:
- quantidade de simulações
- percentual por posição
- indicação de pico de probabilidade

Uso:
distribuição de posições finais no dashboard

---

## Views analíticas

As views organizam e simplificam o consumo do dashboard. Principais exemplos:

- `vw_league_projection`
- `vw_future_matches_next5_2026`
- `vw_team_trend_2026`
- `vw_team_trend_matchday_2026`
- `vw_over_under_achievers_2026`
- `vw_monte_carlo_summary_2026`
- `vw_monte_carlo_neighbors_2026`

Uso:
camada semântica final para o Looker Studio