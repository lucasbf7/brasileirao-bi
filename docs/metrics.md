# Métricas

## PPJ (Pontos por Jogo)

PPJ = pontos / jogos disputados

Usos:

* Forma recente (últimos 5 jogos)
* Histórico por adversário
* Projeção de desempenho

---

## PPJ Final (previsão)

Combinação:

PPJ_final = (w_hist * histórico) + (w_form * forma)

Onde:

* w_hist = 0.7
* w_form = 0.3

---

## Ajuste por dificuldade

Aplicado via `agg_opponent_score_23_25`

Categorias:

* Freguês → boost
* Neutro → neutro
* Carrasco → penalização

---

## Expected Points

### Contínuo

Valor real (ex: 1.33)

### Discreto

Bucket:

* ≥ 2.5 → 3
* ≥ 1.0 → 1
* < 1 → 0

---

## Diferença contínuo vs discreto

Usado para avaliar:

* Agressividade do modelo
* Conservadorismo da projeção
