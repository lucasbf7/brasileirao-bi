# Brasileirão BI

Projeto de análise, projeção e simulação probabilística do Campeonato Brasileiro, construído com Python, BigQuery, Looker Studio e GitHub Actions.

O objetivo do projeto é transformar dados de partidas em uma ferramenta analítica de consulta sobre:

- desempenho atual dos times
- projeção de pontos e posição final
- comparação entre clubes da liga
- cenários probabilísticos de classificação via Monte Carlo

## 🚀 Arquitetura

O fluxo atual do projeto segue esta estrutura:

API de futebol  
→ extração em Python  
→ carga no BigQuery  
→ tabelas tratadas e agregadas  
→ views analíticas  
→ dashboard no Looker Studio

## ⚙️ Stack

- Python
- BigQuery
- SQL
- Looker Studio
- GitHub Actions
- NumPy / pandas

## 🧱 Camadas de dados

### Raw
Recebe os dados brutos vindos da API:
- `raw_matches`
- `raw_teams`

### Curated
Padroniza e reorganiza os dados para uso analítico:
- `cur_matches_2026`
- `cur_team_match_results_2026`
- `dim_teams_latest`
- `agg_team_season_2026`

### Transform
Cria tabelas analíticas e de projeção:
- `matches_enriched`
- `fact_matches_running`
- `agg_form_2026`
- `agg_opponent_score_23_25`
- `dim_future_matches_enriched`
- `fact_projection_matches`
- `fact_projection_micro`

### Monte Carlo
Calcula cenários probabilísticos de posição final:
- `monte_carlo_results_2026`
- `monte_carlo_position_distribution_2026`

### Views analíticas
Abastecem o dashboard:
- `vw_league_projection`
- `vw_future_matches_next5_2026`
- `vw_team_trend_2026`
- `vw_team_trend_matchday_2026`
- `vw_over_under_achievers_2026`
- `vw_monte_carlo_summary_2026`
- `vw_monte_carlo_neighbors_2026`

## ▶️ Setup

Instale as dependências:

```bash
pip install -r requirements.txt
```

Configure as variáveis de ambiente:

```bash
export GOOGLE_APPLICATION_CREDENTIALS=secrets/sua-credential.json
export GCP_PROJECT_ID=brasileirao-bi-analytics
export BQ_DATASET=brasileirao_bi
export BQ_LOCATION=southamerica-east1
export FOOTBALL_DATA_TOKEN=seu_token
```

No Windows/PowerShell, você pode usar o script:

```bash
.\scripts\env.ps1
```

## ▶️ Execução
### 1. Extrair dados

```bash
python -m brasileirao_bi.etl.pipeline --extract
```

### 2. Carregar no BigQuery

```bash
python -m brasileirao_bi.etl.pipeline --load
```

### 3. Criar camada curated

```bash
python -m brasileirao_bi.etl.pipeline --curated
```

### 4. Criar camada transform

```bash
python -m brasileirao_bi.etl.pipeline --transform
```

### 5. Rodar Monte Carlo

```bash
python -m brasileirao_bi.etl.pipeline --monte_carlo
```

### 6. Criar views analíticas

```bash
python -m brasileirao_bi.etl.pipeline --views
```

### Pipeline Completo

```bash
python -m brasileirao_bi.etl.pipeline --all
```

## 📊 O que o dashboard mostra

O dashboard foi organizado em 3 páginas principais:

**1. Cenário do time**
    - pontos atuais
    - pontos projetados
    - PPJ projetado
    - próximos jogos
    - campanha e resumo de gols

**2. Time vs liga**
    - posição atual e projetada
    - tendência recente
    - comparação com os demais clubes
    - classificação projetada da liga

**3. Simulação Monte Carlo**
    - chance de título
    - chance de G4 / G6 / Sul-Americana / rebaixamento
    - distribuição da posição final
    - resumo probabilístico do time
    - comparação com concorrentes diretos

### 🧠 Lógica do modelo

A projeção usa uma abordagem heurística e interpretável, baseada em:
- histórico recente consolidado
- forma atual
- PPJ como métrica central
- ajuste por dificuldade de adversário
- conversão entre expectativa contínua e bucket discreto
- simulação probabilística de cenários finais

### 📌 Limitações
 - Este projeto tem objetivo analítico e exploratório.
 - As projeções não representam previsão exata do futuro.
 - O modelo atual prioriza interpretabilidade, não sofisticação estatística máxima.
 - Desempates por gols/saldo no Monte Carlo ainda usam valores congelados do estado atual.
 - O uso público de dados e imagens deve respeitar os termos da fonte original e os direitos de propriedade intelectual aplicáveis.

### 📚 Documentação complementar

Veja também:
    - `docs/metrics.md`
    - `docs/data_model.md`
    - `docs/decisions/001-ppj-model.md`

### 🔭 Próximos passos
- melhorar a governança e documentação do projeto
- refinar a atualização incremental por temporada
- evoluir a simulação probabilística
- consolidar a versão pública do dashboard