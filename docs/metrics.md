# Métricas

## PPJ (Pontos por Jogo)

PPJ = pontos / jogos disputados

É a métrica central do projeto por ser simples, interpretável e comparável entre contextos.

Usos principais:
- forma recente
- histórico por adversário
- projeção de desempenho
- comparação entre times

---

## PPJ Geral

Representa o desempenho acumulado do time na temporada atual.

Usos:
- leitura do desempenho atual
- comparação com a liga
- base para projeções

---

## PPJ Recente (últimos 5 jogos)

Representa a forma recente do time considerando a janela mais curta de partidas.

Usos:
- medir tendência recente
- comparar forma atual com desempenho acumulado
- enriquecer a leitura de momento do time

---

## PPJ Projetado

Representa o desempenho médio esperado para o restante da temporada.

Usos:
- projeção de pontos finais
- comparação entre times
- construção da tabela projetada

---

## Ajuste por dificuldade

Aplicado via `agg_opponent_score_23_25`.

Categorias:
- Freguês → ajuste favorável
- Neutro → sem ajuste relevante
- Carrasco → ajuste desfavorável

Usos:
- contextualizar confrontos futuros
- ajustar expectativa de pontos
- classificar dificuldade de calendário

---

## Expected Points

### Contínuo
Valor esperado em pontos de forma contínua.
Exemplo: `1.33`

### Discreto
Conversão do valor contínuo para bucket discreto:

- `>= 2.5` → 3 pontos
- `>= 1.0` → 1 ponto
- `< 1.0` → 0 ponto

Usos:
- simplificar leitura de cenário
- gerar “resultado esperado”
- comparar projeção contínua vs discreta

---

## Diferença contínuo vs discreto

Usada para avaliar:
- agressividade da projeção
- conservadorismo do modelo
- impacto da discretização na tabela final

---

## Delta de posição

Diferença entre posição atual e posição projetada.

Leitura adotada:
`delta_posicao = posicao_atual - posicao_projetada`

Exemplos:
- 15 → 10 = `+5` → melhora projetada
- 15 → 17 = `-2` → piora projetada

---

## Métricas probabilísticas (Monte Carlo)

A simulação gera, entre outras:

- `prob_titulo`
- `prob_g4`
- `prob_g6`
- `prob_sulamericana`
- `prob_rebaixamento`
- `most_likely_position`
- `most_likely_position_pct`

Usos:
- leitura de risco
- comparação de cenários
- entendimento da faixa mais provável de classificação final