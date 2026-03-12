# Data Model

## fact_matches_running

Grão: 1 linha por jogo do Vasco

Contém:

* Pontos acumulados
* PPJ acumulado
* PPJ últimos 5

Uso:
Base para forma e projeção

---

## agg_hist_adv_local_23_25

Grão: adversário + casa/fora

Contém:

* PPJ histórico contra adversário
* Saldo de gols

Uso:
Base histórica do modelo

---

## agg_opponent_score_23_25

Grão: adversário + casa/fora

Contém:

* PPJ com shrinkage
* Score de dificuldade
* Categoria (Freguês / Carrasco)

Uso:
Ajuste de dificuldade

---

## dim_future_matches_enriched

Grão: jogos futuros

Contém:

* Features finais do modelo
* PPJ base
* PPJ final

Uso:
Input da projeção

---

## fact_projection_matches

Grão: jogo futuro

Contém:

* Expected points
* Bucket (3/1/0)

---

## fact_projection_micro

Grão: temporada

Contém:

* Projeção total de pontos
* Comparação contínuo vs discreto
